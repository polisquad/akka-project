package streaming.graph.nodes

import akka.actor.{ActorContext, ActorRef}

abstract class Node(val parallelism: Int) {
  var deployed: Vector[ActorRef] = Vector()

  def backWard(downStreams: Vector[ActorRef])(implicit context: ActorContext): Unit
  def initialize(sender: ActorRef): Unit
  def restore(sender: ActorRef, uuid: String): Unit
  def getUpStreams: Vector[ActorRef] = deployed
}