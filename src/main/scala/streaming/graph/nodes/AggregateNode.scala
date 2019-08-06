package streaming.graph.nodes
import akka.actor.{ActorContext, ActorRef}
import streaming.graph.nodes.types.Node.generateName
import streaming.graph.nodes.types.OneToOneNode
import streaming.operators.AggregateOperator

class AggregateNode(parallelism: Int, f: Seq[(String, String)] => (String, String), toAccumulate: Int) extends OneToOneNode(parallelism) {

  override def deploy(downStreams: Vector[ActorRef])(implicit context: ActorContext): Vector[ActorRef] =
    (for (i <- 1 to parallelism)
      yield context.actorOf(AggregateOperator.props(f, downStreams, toAccumulate), generateName("Aggregate", i))).toVector
}

object AggregateNode {

  def apply(parallelism: Int, f: Seq[(String, String)] => (String, String), toAccumulate: Int): AggregateNode =
    new AggregateNode(parallelism, f, toAccumulate)

}
