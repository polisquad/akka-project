package streaming

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import streaming.Streaming.Initializer

import scala.concurrent.Future

// TODO refactor to generalize
// TODO buffer from channel where marker has been received if multiple input channels
class MapOperator(
    f: (String, String) => (String, String),
    downStreams: Vector[ActorRef]
  ) extends Actor with ActorLogging {
  import MapOperator._
  import context.dispatcher

  var upOffsets: Map[ActorRef, Long] = _
  var downOffsets: Map[ActorRef, Long] = _

  // TODO remember to deal with restart
  override def receive: Receive = {
    case Initializer(upStreams) =>
      upOffsets = upStreams.map(x => x -> 0L).toMap
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

  def snapshot(): Unit = {
    // TODO
    log.info("Snapshotting...")
  }

  def operative: Receive = {
    case t: Tuple =>
      log.info(s"Received: $t")
      val expectedOffset = upOffsets(sender())

      if (t.offset == expectedOffset) {
        val (newKey, newValue) = f(t.key, t.value)

        val downStreamOp = downStreams(newValue.hashCode() % downStreams.size)
        val newOffset = downOffsets(downStreamOp)

        val outTuple = Tuple(newKey, newValue, newOffset)
        downStreamOp ! outTuple

        upOffsets = upOffsets.updated(sender(), expectedOffset + 1)
        downOffsets = downOffsets.updated(downStreamOp, newOffset + 1)

        log.info(s"Sent $outTuple to $downStreamOp")
      } else {
        throw new Exception(s"Tuple id was not the expected one. Expected $expectedOffset, Received: ${t.offset}")
      }
  }

}

object MapOperator {

  def props(
    f: (String, String) => (String, String),
    downStreams: Vector[ActorRef],
  ): Props =
    Props(new MapOperator(f, downStreams))

  final case class Tuple(key: String, value: String, offset: Long) {
    override def hashCode(): Int = key.hashCode()
  }

  case object Initialized

}
