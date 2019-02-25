package streaming

import akka.actor.SupervisorStrategy.{Restart, Stop}
import akka.actor.{Actor, ActorLogging, AllForOneStrategy, Props, SupervisorStrategy, Terminated, Timers}
import streaming.Source.{InitializeSource, RestartJob, StartJob}
import streaming.Streaming._

import scala.concurrent.duration._
import java.util.UUID.randomUUID

// TODO test everything
class MasterNode extends Actor with ActorLogging with Timers {
  import MasterNode._
  import context.dispatcher

  var toInitialize: Int = 0
  var toInitializeFromSnapshot: Int = 0
  var lastValidSnapshot: String = _
  var restoreSnapshotTries = 5 // TODO

  override def receive: Receive = {

    case CreateTopology =>
      createTopology()

    case InitializedAck =>
      toInitialize -= 1
      if (toInitialize == 0 ) {
        context.children.foreach(context.watch)
        context.child("Source").get ! StartJob
        // Note: if the source receives this message the graph is up and running
        // The source will send a JobStarted to ack the master the node has started processing
        // If something fails before receiving this message we abort(done in Terminated(actor))
      }

    case JobStarted =>
      timers.cancel(DeployFailureTimer)
      lastValidSnapshot = "start"
      context.become(operative)
      self ! SetSnapshot

    case Terminated(actor) =>
      // If some nodes fail before JobStarted is received
      throw new Exception("Unable to start the Job")

    case DeployFailure =>
      throw new Exception("Unable to start the Job")
  }

  def snapshotRestorer(uuid: String): Receive = {
    case InitializedFromSnapshot(`uuid`) =>
      toInitializeFromSnapshot -= 1
      if (toInitializeFromSnapshot == 0) {
        context.children.foreach(context.watch)
        context.child("Source").get ! RestartJob
      }

    case JobRestarted =>
      timers.cancel(RestoreSnapshotFailureTimer)
      context.become(operative)
      self ! SetSnapshot

    case Terminated(actor) =>
      // If some nodes fail in the meanwhile is received
      throw new Exception("Unable to restore the snapshot")

    case RestoreSnapshotFailure =>
      throw new Exception("Unable to restore the snapshot")
  }

  def operative: Receive = {
    case Terminated(actor) =>
      // requires: we enter here if a child has been stopped by this master node
      context.children.foreach(context.unwatch)
      self ! RestoreLastSnapshot

    case RestoreLastSnapshot =>
      restoreTopology(lastValidSnapshot)
      context.become(snapshotRestorer(lastValidSnapshot))

    case SnapshotDone(uuid) =>
      timers.cancel(SnapshotTimer)
      sender() ! MarkerAck(uuid)
      lastValidSnapshot = uuid
      log.info(s"Performed snapshot with uuid: ${uuid}")
      self ! SetSnapshot

    case SetSnapshot =>
      context.system.scheduler.scheduleOnce(5 seconds) {
        context.child("Source").get ! Marker(randomUUID().toString, 0)
        timers.startSingleTimer(SnapshotTimer, RestoreLastSnapshot, 10 seconds)
      }
  }

  override def supervisorStrategy: SupervisorStrategy =
    AllForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1 seconds) {
      case _ => Stop
    }

  // TODO generalize
  def restoreTopology(uuid: String): Unit = {

    val initializerFromSnapshot = InitializerFromSnapshot(uuid)
    val sink = context.actorOf(Sink.props, "Sink")

    val map21 = context.actorOf(MapOperator.props((s1, s2) => (s1, s2 + "!!!"), Vector(sink)), "Map21")
    val map22 = context.actorOf(MapOperator.props((s1, s2) => (s1, s2 + "!!!"), Vector(sink)), "Map22")

    sink ! initializerFromSnapshot

    val map11 = context.actorOf(MapOperator.props((s1, s2) => (s1, s2.toLowerCase()), Vector(map21, map22)), "Map11")
    val map12 = context.actorOf(MapOperator.props((s1, s2) => (s1, s2.toLowerCase()), Vector(map21, map22)), "Map12")

    map21 ! initializerFromSnapshot
    map22 ! initializerFromSnapshot

    val source = context.actorOf(Source.props(Vector(map11, map12)), "Source")
    source ! InitializeSource

    map11 ! initializerFromSnapshot
    map12 ! initializerFromSnapshot
    toInitializeFromSnapshot = 6

    timers.startSingleTimer(RestoreSnapshotFailureTimer, RestoreSnapshotFailure, 10 seconds)
  }

  // TODO generalize
  def createTopology(): Unit = {
    val sink = context.actorOf(Sink.props, "Sink")

    val map21 = context.actorOf(MapOperator.props((s1, s2) => (s1, s2 + "!!!"), Vector(sink)), "Map21")
    val map22 = context.actorOf(MapOperator.props((s1, s2) => (s1, s2 + "!!!"), Vector(sink)), "Map22")

    val sinkInitializer = Initializer(Vector(map21, map22))
    sink ! sinkInitializer

    val map11 = context.actorOf(MapOperator.props((s1, s2) => (s1, s2.toLowerCase()), Vector(map21, map22)), "Map11")
    val map12 = context.actorOf(MapOperator.props((s1, s2) => (s1, s2.toLowerCase()), Vector(map21, map22)), "Map12")

    val map2Initializer = Initializer(Vector(map11, map12))
    map21 ! map2Initializer
    map22 ! map2Initializer

    val source = context.actorOf(Source.props(Vector(map11, map12)), "Source")
    source ! InitializeSource

    val map1Initializer = Initializer(Vector(source))
    map11 ! map1Initializer
    map12 ! map1Initializer
    toInitialize = 6

    timers.startSingleTimer(DeployFailureTimer, DeployFailure, 10 seconds)
  }
}

object MasterNode {
  val DeployFailureTimer = "DeployFailure"
  val RestoreSnapshotFailureTimer = "RestoreSnapshotFailure"
  val SnapshotTimer = "SnapshotTimeout"

  case object CreateTopology
  case object InitializedAck
  case object DeployFailure
  case object JobStarted
  case object JobRestarted
  case object SetSnapshot
  case object RestoreLastSnapshot
  case object RestoreSnapshotFailure
  final case class SnapshotDone(uuid: String)

  def props: Props = Props(new MasterNode)
}
