package streaming.operators

import akka.actor.{Props, ActorRef}
import streaming.operators.common.Messages.Tuple
import streaming.operators.types.OneToOneOperator

class MapOperator(
  f: (String, String) => (String, String),
  downStreams: Vector[ActorRef]
) extends OneToOneOperator(downStreams) {

  override def processTuple(t: Tuple) = {
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
  def props(
    f: (String, String) => (String, String),
    downStreams: Vector[ActorRef],
  ): Props =
    Props(new MapOperator(f, downStreams))
}
