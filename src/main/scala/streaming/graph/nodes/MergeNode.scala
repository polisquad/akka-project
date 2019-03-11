package streaming.graph.nodes
import akka.actor.{ActorContext, ActorRef}
import streaming.operators.{MergeOperator}

class MergeNode(parallelism: Int, multi: Int) extends MultiToOneNode(parallelism, multi) {

  override def deploy(downStreams: Vector[ActorRef])(implicit context: ActorContext): Vector[ActorRef] = {
    (for (_ <- 1 to parallelism)
      yield context.actorOf(MergeOperator.props(downStreams))).toVector
  }
}

object MergeNode {

  def apply(parallelism: Int, multi: Int): MergeNode = new MergeNode(parallelism, multi)

}
