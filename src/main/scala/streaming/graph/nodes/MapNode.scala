package streaming.graph.nodes
import akka.actor.{ActorContext, ActorRef}
import streaming.graph.nodes.types.OneToOneNode
import streaming.operators.MapOperator

class MapNode(parallelism: Int, f: (String, String) => (String, String)) extends OneToOneNode(parallelism) {

  override def deploy(downStreams: Vector[ActorRef])(implicit context: ActorContext): Vector[ActorRef] = {
    (for (_ <- 1 to parallelism)
      yield context.actorOf(MapOperator.props(f, downStreams))).toVector
  }
}

object MapNode {
  def apply(parallelism: Int, f: (String, String) => (String, String)): MapNode =
    new MapNode(parallelism, f)
}
