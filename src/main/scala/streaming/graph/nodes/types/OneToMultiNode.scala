package streaming.graph.nodes.types

import akka.actor.{ActorContext, ActorRef}
import streaming.operators.common.Messages.{Initializer, RestoreSnapshot}

abstract class OneToMultiNode(parallelism: Int, multi: Int) extends Node(parallelism) {
  private var _prev: Node = _
  private var accumulatedDownStreams: Vector[Vector[ActorRef]] = Vector()
  private var left: Int = multi

  protected def deploy(downStreams: Vector[Vector[ActorRef]])(implicit context: ActorContext): Vector[ActorRef]

  override def backWard(downStreams: Vector[ActorRef])(implicit context: ActorContext): Unit = {
    left -= 1

    if (left == 0) { // Received downStreams from all the splits
      accumulatedDownStreams = accumulatedDownStreams :+ downStreams
      val deployedOperators = deploy(accumulatedDownStreams)
      deployed = deployedOperators
      prev.backWard(deployedOperators)
    } else {
      accumulatedDownStreams = accumulatedDownStreams :+ downStreams
    }
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
