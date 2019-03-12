package streaming.graph.nodes
import akka.actor.{ActorContext, ActorRef}
import streaming.operators.FilterOperator

class FilterNode(parallelism: Int, f: (String, String) => Boolean) extends OneToOneNode(parallelism) {

  override def deploy(downStreams: Vector[ActorRef])(implicit context: ActorContext): Vector[ActorRef] =
    (for (_ <- 1 to parallelism)
      yield context.actorOf(FilterOperator.props(f, downStreams))).toVector
}

object FilterNode {

  def apply(parallelism: Int, f: (String, String) => Boolean): FilterNode =
    new FilterNode(parallelism, f)

}
