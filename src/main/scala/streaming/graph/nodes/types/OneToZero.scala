package streaming.graph.nodes.types

import akka.actor.{ActorContext, ActorRef}
import streaming.operators.common.Messages.{Initializer, RestoreSnapshot}

abstract class OneToZero(parallelism: Int) extends Node(parallelism) {
  private var _prev: Node = _

  protected def deploy()(implicit context: ActorContext): Vector[ActorRef]

  override def backWard(downStreams: Vector[ActorRef])(implicit context: ActorContext): Unit = {
    deployed = deploy()
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

  def prev: Node = _prev
  def prev_=(prev: Node): Unit = _prev = prev
}
