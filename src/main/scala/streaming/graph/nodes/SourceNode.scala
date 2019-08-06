package streaming.graph.nodes
import akka.actor.{ActorContext, ActorRef}
import streaming.graph.nodes.types.Node.generateName
import streaming.graph.nodes.types.ZeroToOneNode
import streaming.operators.SourceOperator

class SourceNode(source: String) extends ZeroToOneNode(1) {

  override def deploy(downStreams: Vector[ActorRef])(implicit context: ActorContext): Vector[ActorRef] = {
    (for (i <- 1 to parallelism)
      yield context.actorOf(SourceOperator.props(downStreams, source), generateName("Source", i))).toVector
  }
}

object SourceNode {
  def apply(source: String): SourceNode = new SourceNode(source)
}
