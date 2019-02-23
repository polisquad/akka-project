package streaming

import akka.actor.SupervisorStrategy.{Restart, Stop}
import akka.actor.{Actor, ActorLogging, AllForOneStrategy, Props, SupervisorStrategy, Terminated, Timers}
import streaming.Source.{InitializeSource, StartJob}
import streaming.Streaming.{InitializeException, Initializer, Marker, MarkerAck}

import scala.concurrent.duration._
import java.util.UUID.randomUUID

class MasterNode extends Actor with ActorLogging with Timers {
  import MasterNode._
  import context.dispatcher

  var toInitialize: Int = 0
  var lastValidSnapshot: String = _

  override def receive: Receive = {
    case CreateTopology =>
      createTopology()

    case InitializedAck =>
      toInitialize -= 1
      if (toInitialize == 0)
        context.child("Source").get ! StartJob

    case JobStarted =>
      timers.cancel(DeployFailureTimer)
      lastValidSnapshot = "start"
      context.become(operative)
      context.children.foreach(context.watch)
      self ! SetSnapshot

    case DeployFailure =>
      throw new Exception("Unable to start the Job")
  }

  def operative: Receive = {
    case Terminated(actor) =>
      // TODO re-deploy architecture giving each operator the last valid checkpoint
      context.children.foreach(context.unwatch)
      self ! RestoreLastSnapshot

    case RestoreLastSnapshot =>
      // TODO
      throw new Exception("end")

    case SnapshotDone(uuid) =>
      timers.cancel("SnapshotTimeout")
      sender() ! MarkerAck
      lastValidSnapshot = uuid
      log.info(s"Performed snapshot with uuid: ${uuid}")

      self ! SetSnapshot


    case SetSnapshot =>
      context.system.scheduler.scheduleOnce(5 seconds) {
        context.child("Source").get ! Marker(randomUUID().toString, 0)
        timers.startSingleTimer("SnapshotTimeout", RestoreLastSnapshot, 10 seconds)
      }
  }

  override def supervisorStrategy: SupervisorStrategy =
    AllForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1 seconds) {
      case InitializeException(_) => Stop // TODO remove this
      case _ => Stop
    }

  // TODO
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
  val SnapshotTimer = "Snapshot"
  case object CreateTopology
  case object InitializedAck
  case object DeployFailure
  case object JobStarted
  case object SetSnapshot
  case object RestoreLastSnapshot
  final case class SnapshotDone(uuid: String)

  def props: Props = Props(new MasterNode)
}
