package streaming.graph.nodes.types

import akka.actor.{ActorContext, ActorRef}
import streaming.operators.common.Messages.RestoreSnapshot
import streaming.operators.types.ZeroToOneOperator.InitializeSource

abstract class ZeroToOneNode(parallelism: Int) extends Node(parallelism) {

  protected def deploy(downStreams: Vector[ActorRef])(implicit context: ActorContext): Vector[ActorRef]

  override def backWard(downStreams: Vector[ActorRef])(implicit context: ActorContext): Unit = {
    deployed = deploy(downStreams)
  }

  override def initialize(sender: ActorRef, uuid: String): Unit = {
    deployed.foreach(_.tell(InitializeSource(uuid), sender))
  }

  override def restore(sender: ActorRef, uuid: String): Unit = {
    deployed.foreach(_.tell(RestoreSnapshot(uuid, Vector()), sender))
  }
}
