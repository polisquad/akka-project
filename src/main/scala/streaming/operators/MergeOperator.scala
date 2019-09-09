package streaming.operators

import akka.actor.{ActorRef, Props}
import streaming.operators.common.Messages.Tuple
import streaming.operators.types.MultiToOneOperator

class MergeOperator[I](downStreams: Vector[ActorRef]) extends MultiToOneOperator[I, I](downStreams) {

  def processTuple(t: Tuple[I]): Unit = {
    val downStreamOp = downStreams(t.key.hashCode() % downStreams.size)
    val newOffset = downOffsets(downStreamOp)

    val outTuple = t.copy(offset = newOffset)
    downStreamOp ! outTuple

    upOffsets = upOffsets.updated(sender(), t.offset + 1)
    downOffsets = downOffsets.updated(downStreamOp, newOffset + 1)

    log.info(s"Sent $outTuple to $downStreamOp")
  }

}

object MergeOperator {
  def props[I](downStreams: Vector[ActorRef]): Props = Props(new MergeOperator(downStreams))
}