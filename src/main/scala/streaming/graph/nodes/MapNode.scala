package streaming.graph.nodes
import akka.actor.{ActorContext, ActorRef, AddressFromURIString, Deploy}
import akka.remote.RemoteScope
import streaming.graph.nodes.types.Node._
import streaming.graph.nodes.types.OneToOneNode
import streaming.operators.MapOperator

class MapNode[I,O](parallelism: Int, f: (String, I) => (String, O), address: String) extends OneToOneNode(parallelism) {

  override def deploy(downStreams: Vector[ActorRef])(implicit context: ActorContext): Vector[ActorRef] = {
    (for (i <- 1 to parallelism)
      yield context.actorOf(
        MapOperator.props(f, downStreams).withDeploy(Deploy(scope = RemoteScope(AddressFromURIString(address)))),
        generateName("Map", i))
    ).toVector
  }
}

object MapNode {
  def apply[I,O](parallelism: Int, f: (String, I) => (String, O), address: String): MapNode[I,O] =
    new MapNode(parallelism, f, address)
}
