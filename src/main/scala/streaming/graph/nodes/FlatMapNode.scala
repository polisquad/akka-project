package streaming.graph.nodes
import akka.actor.{ActorContext, ActorRef, AddressFromURIString, Deploy}
import akka.remote.RemoteScope
import streaming.graph.nodes.types.Node.generateName
import streaming.graph.nodes.types.OneToOneNode
import streaming.operators.FlatMapOperator

class FlatMapNode[I,O](parallelism: Int, f: (String, I) => Seq[(String, O)], address: String) extends OneToOneNode(parallelism) {

  override def deploy(downStreams: Vector[ActorRef])(implicit context: ActorContext): Vector[ActorRef] =
    (for (i <- 1 to parallelism)
      yield context.actorOf(
        FlatMapOperator.props(f, downStreams).withDeploy(Deploy(scope = RemoteScope(AddressFromURIString(address)))),
        generateName("FlatMap", i))
    ).toVector
}

object FlatMapNode {

  def apply[I,O](parallelism: Int, f: (String, I) => Seq[(String, O)], address: String): FlatMapNode[I,O] =
    new FlatMapNode(parallelism, f, address)

}