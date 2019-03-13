package streaming.graph.nodes
import akka.actor.{ActorContext, ActorRef}
import streaming.graph.nodes.types.OneToZero
import streaming.operators.SinkOperator

class SinkNode extends OneToZero(1) {

  override def deploy()(implicit context: ActorContext): Vector[ActorRef] = {
    (for (_ <- 1 to parallelism)
      yield context.actorOf(SinkOperator.props())).toVector
  }
}


object SinkNode {
  def apply(): SinkNode = new SinkNode()
}
