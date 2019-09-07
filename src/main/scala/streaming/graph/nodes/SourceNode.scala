package streaming.graph.nodes
import akka.actor.{ActorContext, ActorRef}
import streaming.graph.nodes.types.Node.generateName
import streaming.graph.nodes.types.ZeroToOneNode
import streaming.operators.SourceOperator
import akka.actor.Deploy
import akka.remote.RemoteScope
import akka.actor.AddressFromURIString

class SourceNode(source: String, address: String) extends ZeroToOneNode(1) {

  override def deploy(downStreams: Vector[ActorRef])(implicit context: ActorContext): Vector[ActorRef] = {
    (for (i <- 1 to parallelism)
      yield context.actorOf(
        SourceOperator.props(downStreams, source).withDeploy(Deploy(scope = RemoteScope(AddressFromURIString(address)))),
        generateName("Source", i))
    ).toVector
  }
}

object SourceNode {
  def apply(source: String, address: String): SourceNode = new SourceNode(source, address)
}
