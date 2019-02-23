package streaming

import akka.actor.{Actor, ActorLogging, ActorRef, Props, Stash, Timers}
import streaming.MapOperator.Tuple
import streaming.MasterNode.JobStarted
import streaming.Streaming._

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}

// TODO refactor to generalize
// TODO add different kind of sources(just replayable)
class Source(downStreams: Vector[ActorRef]) extends Actor with ActorLogging with Stash with Timers {
  import Source._
  import context.dispatcher

  var data: List[(String, String)] = List(
    ("a", "ao"),
    ("b", "bau"),
    ("c", "ciao"),
    ("z", "zeus")
  )
  var offset: Long = 0 // TODO

  var downOffsets: Map[ActorRef, Long] = _
  var takingSnapshot = false

  var markersToAck: Int = _

  override def receive: Receive = {
    case InitializeSource =>
      downOffsets = downStreams.map(x => x -> 0L).toMap

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

  def snapshot(): Unit =
    // TODO
    log.info("Snapshotting...")


  def operative: Receive = {
    case StartJob =>
      self ! Produce
      sender() ! JobStarted

    case Produce =>
      if (takingSnapshot) {
        stash()
      } else {
        if (data != Nil) {
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
      timers.startSingleTimer("MarkersLostTimer", MarkersLost, 2 seconds)

    case MarkersLost =>
      throw new Exception("Markers have been lost")

    case MarkerAck =>
      markersToAck -= 1
      if (markersToAck == 0) {
        log.info("Correctly received marker acks from all the downstream operators")
        timers.cancel("MarkersLostTimer")
        takingSnapshot = false
        unstashAll()
      }

    case SnapshotFailed =>
      throw new Exception("Snapshot failed")

  }

}

object Source {
  def props(downStreams: Vector[ActorRef]): Props = Props(new Source(downStreams))

  case object Produce
  case object InitializeSource
  case object StartJob
}
