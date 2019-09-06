package streaming.operators

import streaming.operators.types.OneToOneOperator
import streaming.operators.common.Messages.Tuple
import akka.actor.{ActorRef, Props}

class FilterOperator[I](
  f: (String, I) => Boolean,
  downStreams: Vector[ActorRef]
) extends OneToOneOperator[I, I](downStreams) {

  override def processTuple(t: Tuple[I]): Unit = {
    val filtered = f(t.key, t.value)

    if (filtered) {

      val downStreamOp = downStreams(t.key.hashCode() % downStreams.size)
      val newOffset = downOffsets(downStreamOp)

      val outTuple = t.copy(offset = newOffset)
      downStreamOp ! outTuple

      downOffsets = downOffsets.updated(downStreamOp, newOffset + 1)
      log.info(s"Sent $outTuple to $downStreamOp")
    }

    upOffsets = upOffsets.updated(sender(), t.offset + 1)
  }
}

object FilterOperator {
  def props[I](f: (String, I) => Boolean, downStreams: Vector[ActorRef]): Props =
    Props(new FilterOperator(f, downStreams))
}