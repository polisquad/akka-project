package streaming.operators

import akka.actor.{Props, ActorRef}
import streaming.operators.types.MultiToOneOperator
import streaming.operators.common.Messages.Tuple

class MergeOperator(downStreams: Vector[ActorRef]) extends MultiToOneOperator(downStreams) {

  def processTuple(t: Tuple): Unit = {
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
  def props(downStreams: Vector[ActorRef]): Props = Props(new MergeOperator(downStreams))
}