package streaming

import akka.actor.{Actor, ActorLogging, ActorRef, Props, Stash, Timers}
import streaming.MasterNode.{JobRestarted, JobStarted}
import streaming.Streaming._
import streaming.operators.MapOperator.Tuple

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}

// TODO refactor to generalize
class Source(downStreams: Vector[ActorRef]) extends Actor with ActorLogging with Stash with Timers {
  import Source._
  import context.dispatcher

  var data: List[(String, String)] = List(
    ("a", "ao"),
    ("b", "bau"),
    ("c", "ciao"),
    ("z", "zeus")
  )
  var offset: Long = 0 // TODO write to file when snapshotting(simulating kafka offset, if restart from scratch read it)

  var downOffsets: Map[ActorRef, Long] = _
  var takingSnapshot = false

  var markersToAck: Int = _
  var uuidToAck: String = _


  def snapshot(): Unit =
    // TODO
    log.info("Snapshotting...")

  def restoreSnapshot(uuid: String): Unit =
    // TODO
    log.info(s"Restoring snapshot ${uuid}...")

  def init(): Unit = {
    downOffsets = downStreams.map(x => x -> 0L).toMap
  }


  override def receive: Receive = {
    case InitializeSource =>
      init()

      Future {
        // Initial starting snapshot
        snapshot()
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
        if (data != Nil) { // TODO change this
          val downStreamOp = downStreams(data.head._1.hashCode() % downStreams.size)
          val newOffset = downOffsets(downStreamOp)

          val outTuple = Tuple(data.head._1, data.head._2, newOffset)

          downStreamOp ! outTuple

          data = data.tail
          log.info(s"Producing: $outTuple")

          downOffsets = downOffsets.updated(downStreamOp, newOffset + 1)
          offset += 1

          self ! Produce
        }
      }

    case Marker(uuid, _) =>
      Future {
        snapshot()
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
      timers.startSingleTimer(MarkersLostTimer, MarkersLost, 2 seconds)

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

object Source {
  val MarkersLostTimer = "MarkersLost"
  def props(downStreams: Vector[ActorRef]): Props = Props(new Source(downStreams))

  case object Produce
  case object InitializeSource
  case object StartJob
  case object RestartJob
}
