package streaming.graph.nodes
import akka.actor.{ActorContext, ActorRef}
import streaming.operators.SplitOperator
import streaming.graph.Graph
import streaming.graph.nodes.types.Node.generateName
import streaming.graph.nodes.types.OneToMultiNode

class SplitNode(parallelism: Int, multi: Int, val subStreams: Seq[Graph]) extends OneToMultiNode(parallelism, multi) {

  override def deploy(downStreams: Vector[Vector[ActorRef]])(implicit context: ActorContext): Vector[ActorRef] = {
    (for (i <- 1 to parallelism)
      yield context.actorOf(SplitOperator.props(downStreams), generateName("Split", i))).toVector
  }
}

object SplitNode {
  def apply(parallelism: Int, multi: Int, subStreams: Seq[Graph]): SplitNode =
    new SplitNode(parallelism, multi, subStreams)
}
