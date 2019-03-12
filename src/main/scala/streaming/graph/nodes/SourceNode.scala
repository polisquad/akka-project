package streaming.graph.nodes
import akka.actor.{ActorContext, ActorRef}
import streaming.Source
import streaming.Source.InitializeSource
import streaming.Streaming.{Initializer, RestoreSnapshot}

class SourceNode extends Node(1) {

  override def backWard(downStreams: Vector[ActorRef])(implicit context: ActorContext): Unit = {
    val deployedOperators = Vector(context.actorOf(Source.props(downStreams)))
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
