package streaming.graph.nodes
import akka.actor.{ActorContext, ActorRef, AddressFromURIString, Deploy}
import akka.remote.RemoteScope
import streaming.graph.nodes.types.Node.generateName
import streaming.graph.nodes.types.OneToZero
import streaming.operators.SinkOperator

class SinkNode(sink: String, address: String) extends OneToZero(1) {

  override def deploy()(implicit context: ActorContext): Vector[ActorRef] = {
    (for (i <- 1 to parallelism)
      yield context.actorOf(
        SinkOperator.props(sink).withDeploy(Deploy(scope = RemoteScope(AddressFromURIString(address)))),
        generateName("Sink", i))
    ).toVector
  }
}


object SinkNode {
  def apply(sink: String, address: String): SinkNode = new SinkNode(sink, address)
}
