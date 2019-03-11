package streaming.graph.nodes
import akka.actor.{ActorContext, ActorRef}
import streaming.operators.{SplitOperator}

class SplitNode(parallelism: Int, multi: Int) extends OneToMultiNode(parallelism, multi) {

  override def deploy(downStreams: Vector[Vector[ActorRef]])(implicit context: ActorContext): Vector[ActorRef] = {
    (for (_ <- 1 to parallelism)
      yield context.actorOf(SplitOperator.props(downStreams))).toVector
  }
}

object SplitNode {
  def apply(parallelism: Int, multi: Int): SplitNode =
    new SplitNode(parallelism, multi)
}
