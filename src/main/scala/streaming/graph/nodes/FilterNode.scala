package streaming.graph.nodes
import akka.actor.{ActorContext, ActorRef}
import streaming.graph.nodes.types.Node.generateName
import streaming.graph.nodes.types.OneToOneNode
import streaming.operators.FilterOperator

class FilterNode[I](parallelism: Int, f: (String, I) => Boolean) extends OneToOneNode(parallelism) {

  override def deploy(downStreams: Vector[ActorRef])(implicit context: ActorContext): Vector[ActorRef] =
    (for (i <- 1 to parallelism)
      yield context.actorOf(FilterOperator.props(f, downStreams), generateName("Filter", i))).toVector
}

object FilterNode {

  def apply[I](parallelism: Int, f: (String, I) => Boolean): FilterNode[I] =
    new FilterNode(parallelism, f)

}
