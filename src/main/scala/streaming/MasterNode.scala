package streaming

import akka.actor.SupervisorStrategy.{Restart, Stop}
import akka.actor.{Actor, ActorContext, ActorLogging, ActorRef, AllForOneStrategy, Props, SupervisorStrategy, Terminated, Timers}
import streaming.Source.{InitializeSource, RestartJob, StartJob}
import streaming.Streaming._

import scala.concurrent.duration._
import java.util.UUID.randomUUID


// TODO test everything
// TODO test, test, test, test, test....
class MasterNode extends Actor with ActorLogging with Timers {
  import MasterNode._
  import context.dispatcher

  var toInitialize: Int = 0
  var toInitializeFromSnapshot: Int = 0
  var lastValidSnapshot: String = _
  var snapshotToAck: String = _
  var children: Children = _

  override def receive: Receive = {

    case CreateTopology =>
      createTopology()

    case InitializedAck =>
      toInitialize -= 1
      if (toInitialize == 0 ) {
        children.watchAll()
        children.source ! StartJob
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

  def snapshotRestorer(uuidToRestore: String): Receive = {
    case RestoredSnapshot(uuid) =>
      if (uuid == uuidToRestore) {
        toInitializeFromSnapshot -= 1
        if (toInitializeFromSnapshot == 0) {
          children.watchAll()
          children.source ! RestartJob
        }
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
      // If we were taking a snapshot and something has failed just cancel the timer since we are going to
      // restore the last snapshot
      timers.cancel(SnapshotTimer)
      children.unwatchAll()
      self ! RestoreLastSnapshot

    case RestoreLastSnapshot =>
      restoreTopology(lastValidSnapshot)
      context.become(snapshotRestorer(lastValidSnapshot))

    case SnapshotDone(uuid) =>
      if (uuid == snapshotToAck) {
        timers.cancel(SnapshotTimer)

        // Just to be coherent with the rest of the nodes in the graph which wait for ack regarding the markers
        // If this ack does not arrive back to the Sink it will fail, but we have the snapshot saved so it's fine
        sender() ! MarkerAck(uuid)
        lastValidSnapshot = uuid
        log.info(s"Performed snapshot with uuid: ${uuid}")
        self ! SetSnapshot
      }

    case SetSnapshot =>
      context.system.scheduler.scheduleOnce(5 seconds) {
        val newSnapshot = randomUUID().toString
        children.source ! Marker(newSnapshot, 0)
        timers.startSingleTimer(SnapshotTimer, RestoreLastSnapshot, 10 seconds)
        snapshotToAck = newSnapshot
      }
  }

  override def supervisorStrategy: SupervisorStrategy =
    AllForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1 seconds) {
      case _ => Stop
    }

  // TODO generalize
  def restoreTopology(uuid: String): Unit = {
    val sink = context.actorOf(Sink.props, "Sink")

    val map21 = context.actorOf(MapOperator.props((s1, s2) => (s1, s2 + "!!!"), Vector(sink)), "Map21")
    val map22 = context.actorOf(MapOperator.props((s1, s2) => (s1, s2 + "!!!"), Vector(sink)), "Map22")

    val sinkInitializer = RestoreSnapshot(uuid, Vector(map21, map22))
    sink ! sinkInitializer

    val map11 = context.actorOf(MapOperator.props((s1, s2) => (s1, s2.toLowerCase()), Vector(map21, map22)), "Map11")
    val map12 = context.actorOf(MapOperator.props((s1, s2) => (s1, s2.toLowerCase()), Vector(map21, map22)), "Map12")

    val map2Initializer = RestoreSnapshot(uuid, Vector(map11, map12))

    map21 ! map2Initializer
    map22 ! map2Initializer

    val source = context.actorOf(Source.props(Vector(map11, map12)), "Source")
    source ! RestoreSnapshot(uuid, Vector()) // TODO change this to RestoreSource

    val map1Initializer = RestoreSnapshot(uuid, Vector(source))
    map11 ! map1Initializer
    map12 ! map1Initializer

    toInitializeFromSnapshot = 6
    children = Children(source, Set(map11, map12, map21, map22), sink)
    timers.startSingleTimer(RestoreSnapshotFailureTimer, RestoreSnapshotFailure, 10 seconds)
  }

  // TODO generalize
  def createTopology(): Unit = {
    val sink = context.actorOf(Sink.props, "Sink")

    var prova: Boolean = false

    val map21 = context.actorOf(MapOperator.props((s1, s2) => {
      if (new scala.util.Random().nextFloat() > 0.5) {
        (s1, s2 + "!!!")
      } else {
        throw new Exception("Failed!")
      }
    }, Vector(sink)), "Map21")
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
    children = Children(source, Set(map11, map12, map21, map22), sink)
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


  case class Children(source: ActorRef, operators: Set[ActorRef], sink: ActorRef) {
    def watchAll()(implicit context: ActorContext): Unit = {
      context.watch(source)
      operators.foreach(context.watch)
      context.watch(sink)
    }

    def unwatchAll()(implicit context: ActorContext): Unit = {
      context.unwatch(source)
      operators.foreach(context.unwatch)
      context.unwatch(sink)
    }

  }
}
