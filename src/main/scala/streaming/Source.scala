package streaming

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import streaming.MapOperator.{Initialized, Tuple}

import scala.concurrent.Future

// TODO temporary source, make it more general, either replayable with offsets or not
class Source(downStreams: Vector[ActorRef]) extends Actor with ActorLogging {
  import Source._
  import context.dispatcher

  var data: List[(String, String)] = List(
    ("a", "ao"),
    ("b", "bau"),
    ("c", "ciao"),
    ("z", "zeus")
  )
  var offset: Long = 0
  var downOffsets: Map[ActorRef, Long] = _

  override def receive: Receive = {
    case InitializeSource =>
      log.info("Here")
      downOffsets = downStreams.map(x => x -> 0L).toMap

      Future {
        // Initial starting snapshot
        snapshot()
      } onComplete {
        _ => self ! Initialized
      }

    case Initialized =>
      context.parent ! MasterNode.InitializedAck
      context.become(operative)
  }

  def snapshot(): Unit = log.info("Snapshotting...")

  def operative: Receive = {
    case Produce =>
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

}

object Source {
  def props(downStreams: Vector[ActorRef]): Props = Props(new Source(downStreams))

  case object Produce
  case object InitializeSource
}
