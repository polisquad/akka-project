package streaming.graph.nodes

import akka.actor.{ActorContext, ActorRef}
import streaming.operators.common.Streaming.{Initializer, RestoreSnapshot}

abstract class OneToOneNode(parallelism: Int) extends Node(parallelism) {
  var prev: Node = _

  def deploy(downStreams: Vector[ActorRef])(implicit context: ActorContext): Vector[ActorRef]

  override def backWard(downStreams: Vector[ActorRef])(implicit context: ActorContext): Unit = {
    deployed = deploy(downStreams)
    prev.backWard(deployed)
  }

  override def initialize(sender: ActorRef): Unit = {
    deployed.foreach(_.tell(Initializer(prev.getUpStreams), sender))
    prev.initialize(sender)
  }

  override def restore(sender: ActorRef, uuid: String): Unit = {
    deployed.foreach(_.tell(RestoreSnapshot(uuid, prev.getUpStreams), sender))
    prev.restore(sender, uuid)
  }
}
