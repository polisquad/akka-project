package streaming

import akka.actor.{Actor, ActorLogging, ActorRef, Props, Stash, Timers}
import streaming.MapOperator.TakeSnapshot
import streaming.MasterNode.SnapshotDone
import streaming.Streaming._

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}

// TODO refactor to generalize
class Sink extends Actor with ActorLogging with Stash with Timers {
  import context.dispatcher

  var upOffsets: Map[ActorRef, Long] = _

  var blockedChannels: Map[ActorRef, Boolean] = _
  var numBlocked: Int = 0

  var filePointer: Long = 0 // TODO write to file like source, if restart from zero read it

  def snapshot(): Unit =
    // TODO
    log.info("Snapshotting...")

  def restoreSnapshot(uuid: String): Unit =
    // TODO
    log.info(s"Restoring snapshot ${uuid}...")

  def init(upStreams: Vector[ActorRef]): Unit = {
    upOffsets = upStreams.map(x => x -> 0L).toMap
    blockedChannels = upStreams.map(x => x -> false).toMap
  }

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
    case t: MapOperator.Tuple =>
      if (blockedChannels(sender())) {
        stash()
      } else {
        log.info(s"$self received $t")
        val expectedOffset = upOffsets(sender())

        if (t.offset == expectedOffset) {
          upOffsets = upOffsets.updated(sender(), expectedOffset + 1)

          // TODO write at current filePointer
          // TODO set new filePointer
          log.info(s"Emitting result: $t")
        } else {
          throw new Exception("Tuple id was not the expected one")
        }
      }

    case TakeSnapshot(uuid) =>
      Future {
        snapshot()
      } onComplete {
        case Success(_) => self ! SnapshotTaken(uuid)
        case Failure(_) => self ! SnapshotFailed
      }

    case SnapshotTaken(uuid) =>
      context.parent ! SnapshotDone(uuid)
      timers.startSingleTimer("MarkersLostTimer", MarkersLost, 2 seconds)

    case MarkerAck(_) => // TODO change this message? it is not really a marker ack
      timers.cancel("MarkersLostTimer")
      blockedChannels = blockedChannels.map {case (k, _) => k -> false}
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

object Sink {
  def props: Props = Props(new Sink)
}
