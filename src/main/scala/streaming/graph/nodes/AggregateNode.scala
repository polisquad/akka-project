package streaming.graph.nodes
import akka.actor.{ActorContext, ActorRef}
import streaming.graph.nodes.types.Node.generateName
import streaming.graph.nodes.types.OneToOneNode
import streaming.operators.AggregateOperator
import akka.actor.Deploy
import akka.remote.RemoteScope
import akka.actor.AddressFromURIString

class AggregateNode[I,O](parallelism: Int, f: Seq[(String, I)] => (String, O), toAccumulate: Int, address: String) extends OneToOneNode(parallelism) {

  override def deploy(downStreams: Vector[ActorRef])(implicit context: ActorContext): Vector[ActorRef] =
    (for (i <- 1 to parallelism)
      yield context.actorOf(
        AggregateOperator.props(f, downStreams, toAccumulate).withDeploy(Deploy(scope = RemoteScope(AddressFromURIString(address)))),
        generateName("Aggregate", i))
    ).toVector
}

object AggregateNode {

  def apply[I,O](parallelism: Int, f: Seq[(String, I)] => (String, O), toAccumulate: Int, address: String): AggregateNode[I,O] =
    new AggregateNode(parallelism, f, toAccumulate, address)

}
