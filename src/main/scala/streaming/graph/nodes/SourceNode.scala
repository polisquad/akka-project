package streaming.graph.nodes
import akka.actor.{ActorContext, ActorRef}
import streaming.operators.common.Streaming.{Initializer, RestoreSnapshot}
import streaming.operators.SourceOperator
import streaming.operators.SourceOperator.InitializeSource

class SourceNode extends Node(1) {

  override def backWard(downStreams: Vector[ActorRef])(implicit context: ActorContext): Unit = {
    val deployedOperators = Vector(context.actorOf(SourceOperator.props(downStreams)))
    deployed = deployedOperators
  }

  override def initialize(sender: ActorRef): Unit = {
    deployed(0).tell(InitializeSource, sender)
  }

  override def restore(sender: ActorRef, uuid: String): Unit = {
    deployed(0).tell(RestoreSnapshot(uuid, Vector()), sender)
  }
}

object SourceNode {
  def apply(): SourceNode = new SourceNode()
}
