package streaming.graph.nodes

import akka.actor.{ActorContext, ActorRef}
import streaming.Streaming.Initializer

abstract class OneToOneNode(parallelism: Int) extends Node(parallelism) {
  var prev: Node = _

  def deploy(downStreams: Vector[ActorRef])(implicit context: ActorContext): Vector[ActorRef]

  override def backWard(downStreams: Vector[ActorRef])(implicit context: ActorContext): Unit = {
    deployed = deploy(downStreams)
    prev.backWard(deployed)
  }

  override def initialize(sender: ActorRef): Unit = {
    deployed.foreach(_.tell(Initializer(prev.getUpStreams), sender))
  }
}
