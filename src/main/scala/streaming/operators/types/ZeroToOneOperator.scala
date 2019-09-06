package streaming.operators.types

import java.io.RandomAccessFile

import akka.actor.{Actor, ActorLogging, ActorRef, Props, Stash, Timers}
import streaming.MasterNode
import streaming.MasterNode.{JobRestarted, JobStarted}
import streaming.operators.common.Messages._
import streaming.operators.common.State

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import streaming.Config

// Represents a generic Source Operator
abstract class ZeroToOneOperator(
  downStreams: Vector[ActorRef],
) extends Actor with ActorLogging with Stash with Timers {
  import ZeroToOneOperator._
  import context.dispatcher

  var downOffsets: Map[ActorRef, Long] = _
  var takingSnapshot = false

  var markersToAck: Int = _
  var uuidToAck: String = _

  def snapshot(uuid: String): Unit = {
    log.info(s"Snapshotting ${uuid}...")
  }

  def restoreSnapshot(uuid: String): Unit = {
    log.info(s"Restoring snapshot ${uuid}...")
  }

  def init(): Unit = {
    downOffsets = downStreams.map(x => x -> 0L).toMap
  }

  override def receive: Receive = {
    case InitializeSource(uuid) =>
      init()

      Future {
        // Initial starting snapshot
        snapshot(uuid)
      } onComplete {
        case Success(_) => self ! Initialized
        case Failure(e) =>
          log.info(f"Initial snapshot failed: ${e}")
          self ! SnapshotFailed
      }

    // TODO change this message ?
    case RestoreSnapshot(uuid, _) =>
      init()

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

  def readNext(): Option[(String, String)]
  def pause(): Unit

  def operative: Receive = {
    case StartJob =>
      self ! Produce
      sender() ! JobStarted

    case RestartJob =>
      self ! Produce
      sender() ! JobRestarted

    case Produce =>
      if (takingSnapshot) {
        stash()
      } else {
        readNext() match {
          case Some((key, value)) =>
            val downStreamOp = downStreams(key.hashCode() % downStreams.size)
            val newOffset = downOffsets(downStreamOp)

            val outTuple = Tuple(key, value, newOffset)

            downStreamOp ! outTuple

            log.info(s"Producing: $outTuple")

            downOffsets = downOffsets.updated(downStreamOp, newOffset + 1)
            self ! Produce

          case None =>
            // Stream of data has been exhausted
            pause()

            // Schedule a produce later in time to see if there is new data to be processed
            context.system.scheduler.scheduleOnce(100 millis) {
              self ! Produce
            }
        }
      }

    case Marker(uuid, _) =>
      Future {
        snapshot(uuid)
      } onComplete {
        case Success(_) => self ! SnapshotTaken(uuid)
        case Failure(_) => self ! SnapshotFailed
      }
      takingSnapshot = true

    case SnapshotTaken(uuid) =>
      downStreams.foreach { downStreamOp =>
        val newOffset = downOffsets(downStreamOp)
        downStreamOp ! Marker(uuid, newOffset)
        downOffsets = downOffsets.updated(downStreamOp, newOffset + 1)
      }
      markersToAck = downStreams.length
      uuidToAck = uuid
      timers.startSingleTimer(MarkersLostTimer, MarkersLost, Config.MarkersTimeout)

    case MarkersLost =>
      throw new Exception("Markers have been lost")

    case MarkerAck(uuid) =>
      if (uuid == uuidToAck) {
        log.info(s"Received marker ack for ${uuid}")
        markersToAck -= 1
        if (markersToAck == 0) {
          log.info("Correctly received marker acks from all the downstream operators")
          timers.cancel(MarkersLostTimer)
          takingSnapshot = false
          unstashAll()
        }
      } else {
        log.info(s"Received unexpected marker ack for ${uuid}")
      }

    case SnapshotFailed =>
      throw new Exception("Snapshot failed")

  }

}

object ZeroToOneOperator {
  val MarkersLostTimer = "MarkersLost"

  case object Produce
  final case class InitializeSource(uuid: String)
  case object StartJob
  case object RestartJob
}
