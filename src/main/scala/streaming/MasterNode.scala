package streaming

import akka.actor.SupervisorStrategy.{Stop}
import akka.actor.{Actor, ActorContext, ActorLogging, ActorRef, AllForOneStrategy, Cancellable, Props, SupervisorStrategy, Terminated, Timers}
import streaming.operators.common.Messages._
import streaming.graph.{GraphBuilder, Graph}

import scala.concurrent.duration._
import java.util.UUID.randomUUID

import streaming.operators.types.ZeroToOneOperator.{RestartJob, StartJob}

class MasterNode(graphBuider: () => Graph) extends Actor with ActorLogging with Timers {
  import MasterNode._
  import context.dispatcher

  var toInitialize: Int = 0
  var toInitializeFromSnapshot: Int = 0
  var lastValidSnapshot: String = _
  var snapshotToAck: String = _
  var children: Children = _
  var graph: Graph = _
  var scheduledSnapshot: Cancellable = Cancellable.alreadyCancelled

  var stopped: Int = 0


  override def receive: Receive = {
    case CreateTopology =>
      createTopology()

    case InitializedAck =>
      if (children.contains(sender())) {
        toInitialize -= 1
        if (toInitialize == 0) {
          children.watchAll()
          children.source ! StartJob
          // Note: if the source receives this message the graph is up and running
          // The source will send a JobStarted to ack the master the node has started processing
          // If something fails before receiving this message we abort(done in Terminated(actor))
        }
      }

    case JobStarted =>
      if (sender() == children.source) {
        timers.cancel(DeployFailureTimer)
        context.become(operative)
        self ! SetSnapshot
      }

    case Terminated(actor) =>
      if (children.contains(actor)) {
        // If some nodes fail before JobStarted is received
        throw new Exception("Unable to start the Job")
      }

    case DeployFailure =>
      throw new Exception("Unable to start the Job")
  }

  def snapshotRestorer(uuidToRestore: String): Receive = {
    case RestoredSnapshot(uuid) =>
      if (children.contains(sender())) {
        if (uuid == uuidToRestore) {
          toInitializeFromSnapshot -= 1
          if (toInitializeFromSnapshot == 0) {
            children.watchAll()
            children.source ! RestartJob
          }
        }
      }

    case JobRestarted =>
      if (sender() == children.source) {
        log.info("Correctly restored snapshot and restarted job")
        timers.cancel(RestoreSnapshotFailureTimer)
        context.become(operative)
        self ! SetSnapshot
      }

    case Terminated(actor) =>
      if (children.contains(actor)) { // double-check (using akka's unwatch/watch should be enough)

        if (stopped == 0) {
          // Graph has been stopped, first terminated message

          log.info("Didn't receive JobRestarted and the graph already failed")
          timers.cancel(RestoreSnapshotFailureTimer)
        }

        stopped += 1
        if (stopped == children.size()) {
          // All the nodes have been stopped

          children.unwatchAll()
          context.become(operative)
          self ! RestoreLastSnapshot
        }
      }

    case RestoreSnapshotFailure =>
      throw new Exception("Unable to restore the snapshot")
  }

  def operative: Receive = {
    case Terminated(actor) =>
      if (children.contains(actor)) { // double-check (using akka's unwatch/watch should be enough)
        if (stopped == 0) {
          // Graph has been stopped, first terminated message

          scheduledSnapshot.cancel()
          timers.cancel(SnapshotTimer)
        }

        stopped += 1
        if (stopped == children.size()) {
          // All the nodes have been stopped

          children.unwatchAll()
          self ! RestoreLastSnapshot
        }
      }

    case RestoreLastSnapshot =>
      log.info(s"Restoring last valid snapshot: ${lastValidSnapshot}")
      restoreTopology(lastValidSnapshot)
      context.become(snapshotRestorer(lastValidSnapshot))

    case SnapshotDone(uuid) =>
      if (sender() == children.sink) {
        if (uuid == snapshotToAck) {
          timers.cancel(SnapshotTimer)

          // If this ack does not arrive back to the Sink it will fail, but we have the snapshot saved so it's fine
          sender() ! MarkerAck(uuid)
          lastValidSnapshot = uuid
          log.info(s"Performed snapshot with uuid: ${uuid}")

          self ! SetSnapshot
        }
      }

    case SetSnapshot =>
      if (!scheduledSnapshot.isCancelled) scheduledSnapshot.cancel()

      scheduledSnapshot = context.system.scheduler.scheduleOnce(Config.SnapshotTime) {

        val newSnapshot = randomUUID().toString
        children.source ! Marker(newSnapshot, 0)
        timers.startSingleTimer(SnapshotTimer, RestoreLastSnapshot, Config.SnapshotTimeout)
        snapshotToAck = newSnapshot
      }
  }

  override def supervisorStrategy: SupervisorStrategy =
    AllForOneStrategy(Config.MaxNrOfRetries, Config.WithinTimeRange) {
      case _ => Stop
    }

  def createTopology(): Unit = {
    val graphBuilder = GraphBuilder(graphBuider())

    lastValidSnapshot = randomUUID().toString
    val graphInfo = graphBuilder.initializeGraph(self, lastValidSnapshot)

    children = Children(graphInfo.source, graphInfo.operators, graphInfo.sink)
    toInitialize = graphInfo.numDeployed
    timers.startSingleTimer(DeployFailureTimer, DeployFailure, Config.DeployTimeout)
    stopped = 0
  }

  def restoreTopology(lastValidSnapshot: String): Unit = {
    val graphBuilder = GraphBuilder(graphBuider())
    val graphInfo = graphBuilder.restoreGraph(self, lastValidSnapshot)

    children = Children(graphInfo.source, graphInfo.operators, graphInfo.sink)
    toInitializeFromSnapshot = graphInfo.numDeployed
    timers.startSingleTimer(RestoreSnapshotFailureTimer, RestoreSnapshotFailure, Config.RestoreSnapshotTimeout)
    stopped = 0
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

  def props(graphBuilder: () => Graph) : Props = Props(new MasterNode(graphBuilder))

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

    def contains(actor: ActorRef): Boolean = {
      source == actor || operators.contains(actor) || sink == actor
    }

    def size(): Int = {
      2 + operators.size
    }

  }
}