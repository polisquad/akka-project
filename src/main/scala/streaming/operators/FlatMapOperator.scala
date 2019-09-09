package streaming.operators

import akka.actor.{ActorRef, Props}
import streaming.operators.common.Messages.Tuple
import streaming.operators.types.OneToOneOperator

class FlatMapOperator[I,O](f: (String, I) => Seq[(String, O)], downStreams: Vector[ActorRef]) extends OneToOneOperator[I,O](downStreams) {

  override def processTuple(t: Tuple[I]): Unit = {
    val newTuples = f(t.key, t.value)

    newTuples.foreach {
      newTuple =>
        val downStreamOp = downStreams(newTuple._1.hashCode() % downStreams.size)
        val newOffset = downOffsets(downStreamOp)

        val outTuple = Tuple(newTuple._1, newTuple._2, newOffset)
        downStreamOp ! outTuple

        downOffsets = downOffsets.updated(downStreamOp, newOffset + 1)

        log.info(s"Sent $outTuple to $downStreamOp")
    }

    upOffsets = upOffsets.updated(sender(), t.offset + 1)
  }
}

object FlatMapOperator {
  def props[I,O](f: (String, I) => Seq[(String, O)], downStreams: Vector[ActorRef]): Props =
    Props(new FlatMapOperator(f, downStreams))
}