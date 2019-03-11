package streaming.graph.nodes
import akka.actor.{ActorContext, ActorRef}
import streaming.Sink
import streaming.Streaming.Initializer

class SinkNode extends Node(0) {
  var prev: Node = _

  override def backWard(downStreams: Vector[ActorRef])(implicit context: ActorContext): Unit = {
    deployed = Vector(context.actorOf(Sink.props()))
    prev.backWard(deployed)
  }

  override def initialize(sender: ActorRef): Unit = {
    deployed(0).tell(Initializer(prev.getUpStreams), sender)
    prev.initialize(sender)
  }
}

object SinkNode {
  def apply(): SinkNode = new SinkNode()
}
