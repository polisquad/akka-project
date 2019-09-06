package streaming.graph.nodes
import akka.actor.{ActorContext, ActorRef}
import streaming.graph.nodes.types.Node.generateName
import streaming.graph.nodes.types.OneToOneNode
import streaming.operators.AggregateOperator

class AggregateNode[I, O](parallelism: Int, f: Seq[(String, I)] => (String, O), toAccumulate: Int) extends OneToOneNode(parallelism) {

  override def deploy(downStreams: Vector[ActorRef])(implicit context: ActorContext): Vector[ActorRef] =
    (for (i <- 1 to parallelism)
      yield context.actorOf(AggregateOperator.props(f, downStreams, toAccumulate), generateName("Aggregate", i))).toVector
}

object AggregateNode {

  def apply[I, O](parallelism: Int, f: Seq[(String, I)] => (String, O), toAccumulate: Int): AggregateNode[I, O] =
    new AggregateNode(parallelism, f, toAccumulate)

}
