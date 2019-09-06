package streaming.operators.types

import java.io.RandomAccessFile

import akka.actor.{Actor, ActorLogging, ActorRef, Props, Stash, Timers}
import streaming.MasterNode
import streaming.MasterNode.SnapshotDone
import streaming.operators.common.Messages._
import streaming.operators.common.State

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import streaming.Config

abstract class OneToZeroOperator[I] extends Actor with ActorLogging with Stash with Timers {
  import context.dispatcher

  var upOffsets: Map[ActorRef, Long] = _

  var blockedChannels: Map[ActorRef, Boolean] = _
  var numBlocked: Int = 0

  var uuidToAck: String = _

  def snapshot(uuid: String): Unit = {
    log.info(s"Snapshotting ${uuid}...")
  }

  def restoreSnapshot(uuid: String): Unit = {
    log.info(s"Restoring snapshot ${uuid}...")
  }

  def init(upStreams: Vector[ActorRef]): Unit = {
    upOffsets = upStreams.map(x => x -> 0L).toMap
    blockedChannels = upStreams.map(x => x -> false).toMap
  }

  override def receive: Receive = {
    case Initializer(uuid, upStreams) =>
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

    case RestoreSnapshot(uuid, upStreams) =>
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

  def writeResult(t: Tuple[I]): Unit

  def operative: Receive = {
    case t: Tuple[I] =>
      if (blockedChannels(sender())) {
        stash()
      } else {
        log.info(s"$self received $t")
        val expectedOffset = upOffsets(sender())

        if (t.offset == expectedOffset) {
          writeResult(t)
        } else {
          throw new Exception("Tuple id was not the expected one")
        }
      }

    case TakeSnapshot(uuid) =>
      Future {
        snapshot(uuid)
      } onComplete {
        case Success(_) => self ! SnapshotTaken(uuid)
        case Failure(_) => self ! SnapshotFailed
      }

    case SnapshotTaken(uuid) =>
      context.parent ! SnapshotDone(uuid)
      uuidToAck = uuid
      timers.startSingleTimer("MarkersLostTimer", MarkersLost, Config.MarkersTimeout)

    case MarkerAck(uuid) =>
      timers.cancel("MarkersLostTimer")
      blockedChannels = blockedChannels.map { case (k, _) => k -> false }
      numBlocked = 0
      unstashAll()

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
  }
}


