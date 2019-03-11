package streaming.graph.nodes
import akka.actor.{ActorContext, ActorRef}
import streaming.Source

class SourceNode extends Node(0) {

  override def backWard(downStreams: Vector[ActorRef])(implicit context: ActorContext): Unit = {
    val deployedOperators = Vector(context.actorOf(Source.props(downStreams)))
    deployed = deployedOperators
  }

  override def initialize(sender: ActorRef): Unit = {}
}

object SourceNode {
  def apply(): SourceNode = new SourceNode()
}
