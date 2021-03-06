package streaming.operators

import akka.actor.{ActorRef, Props}
import streaming.operators.common.Messages.Tuple
import streaming.operators.types.OneToMultiOperator

class SplitOperator[I](downStreams: Vector[Vector[ActorRef]]) extends OneToMultiOperator[I, I](downStreams) {

  def processTuple(t: Tuple[I]): Unit = {
    downStreams.map {
      split =>
        val downStreamOp = t.key.hashCode() % split.size
        split(downStreamOp)
    } foreach {
      downStreamOp =>
        val newOffset = downOffsets(downStreamOp)
        val outTuple = t.copy(offset = newOffset)
        downOffsets = downOffsets.updated(downStreamOp, newOffset + 1)
        downStreamOp ! outTuple
        log.info(s"Sent $outTuple to $downStreamOp")
    }

    upOffsets = upOffsets.updated(sender(), t.offset + 1)
  }
}

object SplitOperator {
  def props[I](downStreams: Vector[Vector[ActorRef]]): Props =
    Props(new SplitOperator(downStreams))
}
