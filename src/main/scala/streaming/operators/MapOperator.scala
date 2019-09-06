package streaming.operators

import akka.actor.{Props, ActorRef}
import streaming.operators.common.Messages.Tuple
import streaming.operators.types.OneToOneOperator

class MapOperator[I,O](f: (String, I) => (String, O), downStreams: Vector[ActorRef]) extends OneToOneOperator[I,O](downStreams) {

  override def processTuple(t: Tuple[I]) = {
    val (newKey, newValue) = f(t.key, t.value)

    val downStreamOp = downStreams(newKey.hashCode() % downStreams.size)
    val newOffset = downOffsets(downStreamOp)

    val outTuple = Tuple(newKey, newValue, newOffset)
    downStreamOp ! outTuple

    upOffsets = upOffsets.updated(sender(), t.offset + 1)
    downOffsets = downOffsets.updated(downStreamOp, newOffset + 1)

    log.info(s"Sent $outTuple to $downStreamOp")
  }
}


object MapOperator {
  def props[I,O](f: (String, I) => (String, O), downStreams: Vector[ActorRef]): Props =
    Props(new MapOperator(f, downStreams))
}
