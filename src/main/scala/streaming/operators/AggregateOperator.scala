package streaming.operators

import akka.actor.{Props, ActorRef}
import streaming.operators.types.OneToOneOperator
import streaming.operators.common.State
import streaming.operators.common.Messages
import streaming.operators.common.Messages.Tuple

class AggregateOperator(
  f: Seq[(String, String)] => (String, String),
  downStreams: Vector[ActorRef],
  toAccumulate: Int
) extends OneToOneOperator(downStreams) {

  var accumulated: Vector[(String, String)] = Vector()

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

  override def processTuple(t: Tuple): Unit = {
    if (accumulated.length < toAccumulate) {
      // accumulate
      accumulated = accumulated :+ (t.key, t.value)
    } else {
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
  def props(f: Seq[(String, String)] => (String, String), downStreams: Vector[ActorRef], toAccumulate: Int): Props =
    Props(new AggregateOperator(f, downStreams, toAccumulate))
}