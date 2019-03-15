package streaming.graph.nodes.types

import akka.actor.{ActorContext, ActorRef}

abstract class Node(val parallelism: Int) {
  protected var _deployed: Vector[ActorRef] = Vector()

  def backWard(downStreams: Vector[ActorRef])(implicit context: ActorContext): Unit
  def initialize(sender: ActorRef): Unit
  def restore(sender: ActorRef, uuid: String): Unit
  def getUpStreams: Vector[ActorRef] = _deployed

  def deployed: Vector[ActorRef] = _deployed
  def deployed_=(deployed: Vector[ActorRef]): Unit = _deployed = deployed
}