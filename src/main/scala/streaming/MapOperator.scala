package streaming

import akka.actor.{Actor, ActorLogging, ActorRef, Props, Stash, Timers}
import streaming.Streaming._
import scala.concurrent.duration._

import scala.concurrent.Future
import scala.util.{Failure, Success}

// TODO refactor to generalize
class MapOperator(
    f: (String, String) => (String, String),
    downStreams: Vector[ActorRef]
  ) extends Actor with ActorLogging with Stash with Timers {
  import MapOperator._
  import context.dispatcher

  var upOffsets: Map[ActorRef, Long] = _
  var downOffsets: Map[ActorRef, Long] = _

  var blockedChannels: Map[ActorRef, Boolean] = _
  var numBlocked: Int = 0

  var markersToAck: Int = _

  // TODO remember to deal with restart
  override def receive: Receive = {
    case Initializer(upStreams) =>
      upOffsets = upStreams.map(x => x -> 0L).toMap
      downOffsets = downStreams.map(x => x -> 0L).toMap
      blockedChannels = upStreams.map(x => x -> false).toMap

      Future {
        // Initial starting snapshot
        snapshot()
      } onComplete {
        case Success(_) => self ! Initialized
        case Failure(_) => self ! SnapshotFailed
      }

    case Initialized =>
      context.parent ! MasterNode.InitializedAck
      context.become(operative)

    case SnapshotFailed =>
      throw InitializeException("Initial snapshot failed")
  }


  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    super.preRestart(reason, message)
    log.info("Restarting")
  }

  def snapshot(): Unit = {
    // TODO
    log.info("Snapshotting...")
  }

  def operative: Receive = {
    case t: Tuple =>
      if (blockedChannels(sender())) {
        log.info("Stashing")
        stash()
      } else {
        log.info(s"Received: $t")
        val expectedOffset = upOffsets(sender())

        if (t.offset == expectedOffset) {
          val (newKey, newValue) = f(t.key, t.value)

          val downStreamOp = downStreams(newValue.hashCode() % downStreams.size)
          val newOffset = downOffsets(downStreamOp)

          val outTuple = Tuple(newKey, newValue, newOffset)
          downStreamOp ! outTuple

          upOffsets = upOffsets.updated(sender(), expectedOffset + 1)
          downOffsets = downOffsets.updated(downStreamOp, newOffset + 1)

          log.info(s"Sent $outTuple to $downStreamOp")
        } else {
          throw new Exception(s"Tuple id was not the expected one. Expected $expectedOffset, Received: ${t.offset}")
        }
      }

    case marker @ Marker(uuid, offset) =>
      log.info(s"Received marker ${marker}")
      val expectedOffset = upOffsets(sender())

      if (offset == expectedOffset) {
        blockedChannels = blockedChannels.updated(sender(), true)
        numBlocked += 1

        if (numBlocked == upOffsets.size) {
          self ! TakeSnapshot(uuid)
        }

        sender() ! MarkerAck
        upOffsets = upOffsets.updated(sender(), expectedOffset + 1)

      } else {
        throw new Exception(s"Marker id was not the expected one. Expected $expectedOffset, Received: ${marker.offset}")
      }

    case TakeSnapshot(uuid) =>
      Future {
        snapshot()
      } onComplete {
        case Success(_) => self ! SnapshotTaken(uuid)
        case Failure(_) => self ! SnapshotFailed
      }

    case SnapshotTaken(uuid) =>
      downStreams.foreach { downStreamOp =>
        val newOffset = downOffsets(downStreamOp)
        downStreamOp ! Marker(uuid, newOffset)
        downOffsets = downOffsets.updated(downStreamOp, newOffset + 1)
      }
      markersToAck = downStreams.size
      timers.startSingleTimer("MarkersLostTimer", MarkersLost, 2 seconds)

    case MarkersLost =>
      throw new Exception("Markers have been lost")

    case SnapshotFailed =>
      throw new Exception("Snapshot failed")

    case MarkerAck =>
      markersToAck -= 1
      if (markersToAck == 0) {
        log.info("Correctly received marker acks from all the downstream operators")
        timers.cancel("MarkersLostTimer")
        blockedChannels = blockedChannels.map {case (k, _) => k -> false}
        numBlocked = 0
        unstashAll()
      }
  }

}

object MapOperator {

  def props(
    f: (String, String) => (String, String),
    downStreams: Vector[ActorRef],
  ): Props =
    Props(new MapOperator(f, downStreams))

  final case class Tuple(key: String, value: String, offset: Long) {
    override def hashCode(): Int = key.hashCode()
  }

  final case class TakeSnapshot(uuid: String)
}
