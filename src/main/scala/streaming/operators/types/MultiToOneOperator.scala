package streaming.operators.types

import akka.actor.{Actor, ActorLogging, ActorRef, Props, Stash, Timers}
import streaming.MasterNode
import streaming.operators.common.Messages._

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import streaming.Config

abstract class MultiToOneOperator[I,O](downStreams: Vector[ActorRef]) extends Operator {
  import context.dispatcher

  var upOffsets: Map[ActorRef, Long] = _
  var downOffsets: Map[ActorRef, Long] = _

  var blockedChannels: Map[ActorRef, Boolean] = _
  var numBlocked: Int = 0

  var markersToAck: Int = _
  var uuidToAck: String = _

  def init(upStreams: Vector[Vector[ActorRef]]): Unit = {
    val flattenedUpStreams = upStreams.flatten
    upOffsets = flattenedUpStreams.map(x => x -> 0L).toMap
    downOffsets = downStreams.map(x => x -> 0L).toMap
    blockedChannels = flattenedUpStreams.map(x => x -> false).toMap
  }

  def snapshot(uuid: String): Unit =
    log.info(s"Snapshotting ${uuid}...")

  def restoreSnapshot(uuid: String): Unit =
    log.info(s"Restoring snapshot ${uuid}...")

  override def receive: Receive = {
    case MultiInitializer(uuid, upStreams) =>
      init(upStreams)

      Future {
        // Initial starting snapshot
        snapshot(uuid)
      } onComplete {
        case Success(_) => self ! Initialized
        case Failure(e) =>
          log.info(f"Initial snapshot failed: ${e}")
          self ! SnapshotFailed
      }

    case MultiRestoreSnapshot(uuid, upStreams) =>
      init(upStreams)

      Future {
        restoreSnapshot(uuid)
      } onComplete {
        case Success(_) => self ! RestoredSnapshot(uuid)
        case Failure(e) =>
          log.info(f"Restore snapshot failed: ${e}")
          self ! RestoreSnapshotFailed
      }

    case Initialized =>
      context.parent ! MasterNode.InitializedAck
      context.become(operative)

    case SnapshotFailed =>
      throw new Exception("Initial snapshot failed")

    case RestoredSnapshot(uuid) =>
      context.parent ! RestoredSnapshot(uuid)
      context.become(operative)

    case RestoreSnapshotFailed =>
      throw new Exception("Restore snapshot failed")
  }

  def processTuple(t: Tuple[I]): Unit

  def operative: Receive = {
    case t: Tuple[I] =>
      if (blockedChannels(sender())) {
        stash()
      } else {
        log.info(s"Received: $t")
        val expectedOffset = upOffsets(sender())

        if (t.offset == expectedOffset) {
          processTuple(t)
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

        sender() ! MarkerAck(uuid)
        upOffsets = upOffsets.updated(sender(), expectedOffset + 1)

      } else {
        throw new Exception(s"Marker id was not the expected one. Expected $expectedOffset, Received: ${marker.offset}")
      }

    case TakeSnapshot(uuid) =>
      Future {
        snapshot(uuid)
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
      uuidToAck = uuid
      timers.startSingleTimer("MarkersLostTimer", MarkersLost, Config.MarkersTimeout)

    case MarkersLost =>
      throw new Exception("Markers have been lost")

    case SnapshotFailed =>
      throw new Exception("Snapshot failed")

    case MarkerAck(uuid) =>
      if (uuid == uuidToAck) {
        log.info(s"Received marker ack for ${uuid}")
        markersToAck -= 1
        if (markersToAck == 0) {
          log.info("Correctly received marker acks from all the downstream operators")
          timers.cancel("MarkersLostTimer")
          blockedChannels = blockedChannels.map { case (k, _) => k -> false }
          numBlocked = 0
          unstashAll()
        }
      } else {
        log.info(s"Received unexpected marker ack for ${uuid}")
      }
  }
}
