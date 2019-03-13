package streaming.graph.nodes
import akka.actor.{ActorContext, ActorRef}
import streaming.operators.SplitOperator
import streaming.graph.Stream
import streaming.graph.nodes.types.OneToMultiNode

class SplitNode(parallelism: Int, multi: Int, val subStreams: Seq[Stream]) extends OneToMultiNode(parallelism, multi) {

  override def deploy(downStreams: Vector[Vector[ActorRef]])(implicit context: ActorContext): Vector[ActorRef] = {
    (for (_ <- 1 to parallelism)
      yield context.actorOf(SplitOperator.props(downStreams))).toVector
  }
}

object SplitNode {
  def apply(parallelism: Int, multi: Int, subStreams: Seq[Stream]): SplitNode =
    new SplitNode(parallelism, multi, subStreams)
}
