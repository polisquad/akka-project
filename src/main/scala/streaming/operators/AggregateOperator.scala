package streaming.operators

import akka.actor.{ActorRef, Props}
import streaming.operators.common.Messages.Tuple
import streaming.operators.common.State
import streaming.operators.types.OneToOneOperator

class AggregateOperator[I,O](f: Seq[(String, I)] => (String, O), downStreams: Vector[ActorRef], toAccumulate: Int) extends OneToOneOperator[I,O](downStreams) {

  var accumulated: Vector[(String, I)] = Vector()

  override def snapshot(uuid: String): Unit = {
    log.info(s"Snapshotting ${uuid}...")

    State.writeVector(accumulated, uuid + "aggregate-accumulated.txt")

    log.info(f"Written to state accumulated: ${accumulated}")
  }

  override def restoreSnapshot(uuid: String): Unit = {
    log.info(s"Restoring snapshot ${uuid}...")

    accumulated = State.readVector(uuid + "aggregate-accumulated.txt")

    log.info(f"Restored accumulated: ${accumulated}")
  }

  override def processTuple(t: Tuple[I]): Unit = {
    accumulated = accumulated :+ (t.key, t.value)

    if (accumulated.length == toAccumulate) {
      // we can now compute the aggregate result and send it downstream
      val aggregateResult = f(accumulated)
      accumulated = Vector()

      val downStreamOp = downStreams(aggregateResult._1.hashCode() % downStreams.size)
      val newOffset = downOffsets(downStreamOp)

      val outTuple = Tuple(aggregateResult._1, aggregateResult._2, newOffset)
      downStreamOp ! outTuple

      downOffsets = downOffsets.updated(downStreamOp, newOffset + 1)

      log.info(s"Sent $outTuple to $downStreamOp")
    }

    upOffsets = upOffsets.updated(sender(), t.offset + 1)
  }
}

object AggregateOperator {
  def props[I,O](f: Seq[(String, I)] => (String, O), downStreams: Vector[ActorRef], toAccumulate: Int): Props =
    Props(new AggregateOperator(f, downStreams, toAccumulate))
}