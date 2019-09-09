package streaming.graph.nodes
import akka.actor.{ActorContext, ActorRef, AddressFromURIString, Deploy}
import akka.remote.RemoteScope
import streaming.graph.nodes.types.MultiToOneNode
import streaming.graph.nodes.types.Node.generateName
import streaming.operators.MergeOperator

class MergeNode[I](parallelism: Int, multi: Int, address: String) extends MultiToOneNode(parallelism, multi) {

  override def deploy(downStreams: Vector[ActorRef])(implicit context: ActorContext): Vector[ActorRef] = {
    (for (i <- 1 to parallelism)
      yield context.actorOf(
        MergeOperator.props(downStreams).withDeploy(Deploy(scope = RemoteScope(AddressFromURIString(address)))),
        generateName("Merge", i))
    ).toVector
  }
}

object MergeNode {
  def apply[I](parallelism: Int, multi: Int, address: String): MergeNode[I] =
    new MergeNode(parallelism, multi, address)
}
