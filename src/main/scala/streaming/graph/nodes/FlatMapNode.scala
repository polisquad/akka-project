package streaming.graph.nodes
import akka.actor.{ActorContext, ActorRef}
import streaming.graph.nodes.types.OneToOneNode
import streaming.operators.FlatMapOperator

class FlatMapNode(parallelism: Int, f: (String, String) => Seq[(String, String)]) extends OneToOneNode(parallelism) {

  override def deploy(downStreams: Vector[ActorRef])(implicit context: ActorContext): Vector[ActorRef] =
    (for (_ <- 1 to parallelism)
      yield context.actorOf(FlatMapOperator.props(f, downStreams))).toVector
}

object FlatMapNode {

  def apply(parallelism: Int, f: (String, String) => Seq[(String, String)]): FlatMapNode =
    new FlatMapNode(parallelism, f)

}