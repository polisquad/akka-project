package streaming.graph.nodes
import akka.actor.{ActorContext, ActorRef}
import streaming.graph.nodes.types.OneToOneNode
import streaming.operators.MapOperator
import streaming.graph.nodes.types.Node._

class MapNode[I,O](parallelism: Int, f: (String, I) => (String, O)) extends OneToOneNode(parallelism) {

  override def deploy(downStreams: Vector[ActorRef])(implicit context: ActorContext): Vector[ActorRef] = {
    (for (i <- 1 to parallelism)
      yield context.actorOf(MapOperator.props(f, downStreams), generateName("Map", i))).toVector
  }
}

object MapNode {
  def apply[I,O](parallelism: Int, f: (String, I) => (String, O)): MapNode[I,O] =
    new MapNode(parallelism, f)
}
