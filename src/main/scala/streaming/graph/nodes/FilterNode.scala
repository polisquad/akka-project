package streaming.graph.nodes
import akka.actor.{ActorContext, ActorRef}
import streaming.graph.nodes.types.Node.generateName
import streaming.graph.nodes.types.OneToOneNode
import streaming.operators.FilterOperator
import akka.actor.Deploy
import akka.remote.RemoteScope
import akka.actor.AddressFromURIString

class FilterNode[I](parallelism: Int, f: (String, I) => Boolean, address: String) extends OneToOneNode(parallelism) {

  override def deploy(downStreams: Vector[ActorRef])(implicit context: ActorContext): Vector[ActorRef] =
    (for (i <- 1 to parallelism)
      yield context.actorOf(
        FilterOperator.props(f, downStreams).withDeploy(Deploy(scope = RemoteScope(AddressFromURIString(address)))),
        generateName("Filter", i))
    ).toVector
}

object FilterNode {

  def apply[I](parallelism: Int, f: (String, I) => Boolean, address: String): FilterNode[I] =
    new FilterNode(parallelism, f, address)

}
