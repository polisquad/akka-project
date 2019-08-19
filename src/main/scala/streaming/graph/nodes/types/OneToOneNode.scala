package streaming.graph.nodes.types

import akka.actor.{ActorContext, ActorRef}
import streaming.operators.common.Messages.{Initializer, RestoreSnapshot}

abstract class OneToOneNode(parallelism: Int) extends Node(parallelism) {
  private var _prev: Node = _

  protected def deploy(downStreams: Vector[ActorRef])(implicit context: ActorContext): Vector[ActorRef]

  override def backWard(downStreams: Vector[ActorRef])(implicit context: ActorContext): Unit = {
    deployed = deploy(downStreams)
    prev.backWard(deployed)
  }

  override def initialize(sender: ActorRef, uuid: String): Unit = {
    deployed.foreach(_.tell(Initializer(uuid, prev.getUpStreams), sender))
    prev.initialize(sender, uuid)
  }

  override def restore(sender: ActorRef, uuid: String): Unit = {
    deployed.foreach(_.tell(RestoreSnapshot(uuid, prev.getUpStreams), sender))
    prev.restore(sender, uuid)
  }

  def prev: Node = _prev
  def prev_=(prev: Node): Unit = _prev = prev
}
