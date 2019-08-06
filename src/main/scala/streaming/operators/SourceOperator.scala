package streaming.operators

import java.io.RandomAccessFile

import akka.actor.{Actor, ActorLogging, ActorRef, Props, Stash, Timers}
import streaming.MasterNode
import streaming.MasterNode.{JobRestarted, JobStarted}
import streaming.operators.common.Messages._
import streaming.operators.common.State

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}

// TODO refactor to generalize
class SourceOperator(downStreams: Vector[ActorRef], source: String) extends Actor with ActorLogging with Stash with Timers {
  import SourceOperator._
  import context.dispatcher

  var data: RandomAccessFile = _

  var offset: Long = 0

  var downOffsets: Map[ActorRef, Long] = _
  var takingSnapshot = false

  var markersToAck: Int = _
  var uuidToAck: String = _


  def snapshot(uuid: String): Unit = {
    log.info(s"Snapshotting ${uuid}...")

    State.writeLong(offset, uuid + "source-offset.txt")

    log.info(f"Written to state offset: ${offset}")
  }

  def restoreSnapshot(uuid: String): Unit = {
    log.info(s"Restoring snapshot ${uuid}...")

    offset = State.readLong(uuid + "source-offset.txt")

    log.info(f"Restored offset: ${offset}")
  }

  def init(): Unit = {
    downOffsets = downStreams.map(x => x -> 0L).toMap
  }


  override def receive: Receive = {
    case InitializeSource =>
      init()

      Future {
        // Initial starting snapshot
        snapshot("start")
      } onComplete {
        case Success(_) => self ! Initialized
        case Failure(_) => self ! SnapshotFailed
      }

    // TODO change this message ?
    case RestoreSnapshot(uuid, _) =>
      init()

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
            data.close()
            data = null

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
      timers.startSingleTimer(MarkersLostTimer, MarkersLost, 5 seconds)

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

  def readNext(): Option[(String, String)] = {
    if (data == null) {
      data = new RandomAccessFile(source, "r")
    }

    if (offset < data.length()) {
      data.seek(offset)
      val line = data.readLine()
      val lineSplit = line.split(",")

      offset += line.length + 1
      Some((lineSplit(0), lineSplit(1)))
    } else {
      None
    }
  }

}

object SourceOperator {
  val MarkersLostTimer = "MarkersLost"
  def props(downStreams: Vector[ActorRef], source: String): Props = Props(new SourceOperator(downStreams, source))

  case object Produce
  case object InitializeSource
  case object StartJob
  case object RestartJob
}
