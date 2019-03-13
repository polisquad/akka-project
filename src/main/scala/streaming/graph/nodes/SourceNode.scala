package streaming.graph.nodes
import akka.actor.{ActorContext, ActorRef}
import streaming.graph.nodes.types.ZeroToOneNode
import streaming.operators.SourceOperator

class SourceNode extends ZeroToOneNode(1) {

  override def deploy(downStreams: Vector[ActorRef])(implicit context: ActorContext): Vector[ActorRef] = {
    (for (_ <- 1 to parallelism)
      yield context.actorOf(SourceOperator.props(downStreams))).toVector
  }
}

object SourceNode {
  def apply(): SourceNode = new SourceNode()
}
