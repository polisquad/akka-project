package streaming

import akka.actor.SupervisorStrategy.{Restart, Stop}
import akka.actor.{Actor, ActorContext, ActorLogging, ActorRef, AllForOneStrategy, Cancellable, Props, SupervisorStrategy, Terminated, Timers}
import streaming.Source.{RestartJob, StartJob}
import streaming.Streaming._
import streaming.graph.{GraphBuilder, Stream}

import scala.concurrent.duration._
import java.util.UUID.randomUUID


// TODO test everything
// TODO test, test, test, test, test....
// TODO test different kind of topology
class MasterNode(streamBuilder: () => Stream) extends Actor with ActorLogging with Timers {
  import MasterNode._
  import context.dispatcher

  var toInitialize: Int = 0
  var toInitializeFromSnapshot: Int = 0
  var lastValidSnapshot: String = _
  var snapshotToAck: String = _
  var children: Children = _
  var graph: Stream = _ // TODO change also the name Stream to graph
  var scheduledSnapshot: Cancellable = Cancellable.alreadyCancelled

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
      scheduledSnapshot.cancel()
      timers.cancel(SnapshotTimer)
      children.unwatchAll()
      self ! RestoreLastSnapshot

    case RestoreLastSnapshot =>
      restoreTopology(lastValidSnapshot)
      log.info(s"Restoring ${lastValidSnapshot}")
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
      if (!scheduledSnapshot.isCancelled)
        scheduledSnapshot.cancel()

      scheduledSnapshot = context.system.scheduler.scheduleOnce(10 seconds) {
        val newSnapshot = randomUUID().toString
        children.source ! Marker(newSnapshot, 0)
        timers.startSingleTimer(SnapshotTimer, RestoreLastSnapshot, 10 seconds)
        snapshotToAck = newSnapshot
      }
  }

  override def supervisorStrategy: SupervisorStrategy =
    AllForOneStrategy(maxNrOfRetries = 1, withinTimeRange = 1 seconds) {
      case _ => Stop
    }

  def createTopology(): Unit = {
    val graphBuilder = GraphBuilder(streamBuilder())
    val graphInfo = graphBuilder.initializeGraph(self)

    children = Children(graphInfo.source, graphInfo.operators, graphInfo.sink)
    toInitialize = graphInfo.numDeployed
    timers.startSingleTimer(DeployFailureTimer, DeployFailure, 10 seconds)
  }

  def restoreTopology(lastValidSnapshot: String): Unit = {
    val graphBuilder = GraphBuilder(streamBuilder())
    val graphInfo = graphBuilder.restoreGraph(self, lastValidSnapshot)

    children = Children(graphInfo.source, graphInfo.operators, graphInfo.sink)
    toInitializeFromSnapshot = graphInfo.numDeployed
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

  def props(streamBuilder: () => Stream) : Props = Props(new MasterNode(streamBuilder))


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
