package streaming.operators

import akka.actor.{Props, ActorRef}
import streaming.operators.types.OneToOneOperator
import streaming.operators.common.Messages
import streaming.operators.common.Messages.Tuple

class FlatMapOperator(
  f: (String, String) => Seq[(String, String)],
  downStreams: Vector[ActorRef]
) extends OneToOneOperator(downStreams) {

  override def processTuple(t: Messages.Tuple): Unit = {
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
  def props(f: (String, String) => Seq[(String, String)], downStreams: Vector[ActorRef]): Props =
    Props(new FlatMapOperator(f, downStreams))
}