package streaming

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import streaming.MapOperator.{Initialized, Tuple}
import streaming.Streaming.Initializer

import scala.concurrent.Future

// TODO temporary sink, make it more general
class Sink extends Actor with ActorLogging {
  import context.dispatcher

  var upOffsets: Map[ActorRef, Long] = _

  override def receive: Receive = {
    case Initializer(upStreams) =>
      upOffsets = upStreams.map(x => x -> 0L).toMap

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

  def snapshot(): Unit = {
    // TODO
    log.info("Snapshotting...")
  }

  def operative: Receive = {
    case t: MapOperator.Tuple =>
      log.info(s"$self received $t")
      val expectedOffset = upOffsets(sender())

      if (t.offset == expectedOffset) {
        upOffsets = upOffsets.updated(sender(), expectedOffset + 1)
        log.info(s"Emitting result: $t")
      } else {
        throw new Exception("Tuple id was not the expected one")
      }
  }

}

object Sink {
  def props: Props = Props(new Sink)
}
