package streaming.operators

import akka.actor.{Actor, ActorLogging, ActorRef, Props, Stash, Timers}
import streaming.MasterNode
import streaming.operators.common.Streaming._
import streaming.operators.MapOperator.{TakeSnapshot}

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}

// TODO test
class FilterOperator(
  f: (String, String) => Boolean,
  downStreams: Vector[ActorRef]
  ) extends Actor with ActorLogging with Stash with Timers {
  import context.dispatcher

  var upOffsets: Map[ActorRef, Long] = _
  var downOffsets: Map[ActorRef, Long] = _

  var blockedChannels: Map[ActorRef, Boolean] = _
  var numBlocked: Int = 0

  var markersToAck: Int = _
  var uuidToAck: String = _

  def init(upStreams: Vector[ActorRef]): Unit = {
    upOffsets = upStreams.map(x => x -> 0L).toMap
    downOffsets = downStreams.map(x => x -> 0L).toMap
    blockedChannels = upStreams.map(x => x -> false).toMap
  }

  def snapshot(): Unit =
  // TODO
    log.info("Snapshotting...")

  def restoreSnapshot(uuid: String): Unit =
  // TODO
    log.info(s"Restoring snapshot ${uuid}...")

  override def receive: Receive = {
    case Initializer(upStreams) =>
      init(upStreams)

      Future {
        // Initial starting snapshot
        snapshot()
      } onComplete {
        case Success(_) => self ! Initialized
        case Failure(_) => self ! SnapshotFailed
      }

    case RestoreSnapshot(uuid, upStreams) =>
      init(upStreams)

      Future {
        restoreSnapshot(uuid)
      } onComplete {
        case Success(_) => self ! RestoredSnapshot(uuid)
        case Failure(_) => self ! RestoreSnapshotFailed
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

  def operative: Receive = {
    case t: Tuple =>
      if (blockedChannels(sender())) {
        log.info("Stashing")
        stash()
      } else {
        log.info(s"Received: $t")
        val expectedOffset = upOffsets(sender())

        if (t.offset == expectedOffset) {
          val filtered = f(t.key, t.value)

          if (!filtered) {

            val downStreamOp = downStreams(t.key.hashCode() % downStreams.size)
            val newOffset = downOffsets(downStreamOp)

            val outTuple = t.copy(offset = newOffset)
            downStreamOp ! outTuple

            downOffsets = downOffsets.updated(downStreamOp, newOffset + 1)
            log.info(s"Sent $outTuple to $downStreamOp")
          }

          upOffsets = upOffsets.updated(sender(), expectedOffset + 1)

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
      uuidToAck = uuid
      timers.startSingleTimer("MarkersLostTimer", MarkersLost, 2 seconds)

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

object FilterOperator {
  def props(f: (String, String) => Boolean, downStreams: Vector[ActorRef]): Props =
    Props(new FilterOperator(f, downStreams))

}
