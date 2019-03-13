package streaming.graph.nodes

import akka.actor.{ActorContext, ActorRef}
import streaming.operators.common.Streaming.{MultiInitializer, MultiRestoreSnapshot}

import scala.annotation.tailrec

abstract class MultiToOneNode(parallelism: Int, multi: Int) extends Node(parallelism) {
  var prevs: Vector[Node] = Vector()
  val left: Int = multi

  def deploy(downStreams: Vector[ActorRef])(implicit context: ActorContext): Vector[ActorRef]

  override def backWard(downStreams: Vector[ActorRef])(implicit context: ActorContext): Unit = {
    deployed = deploy(downStreams)
    prevs.foreach { prev => prev.backWard(deployed) }
  }

  @tailrec
  private def gatherUpStreams(prevs: Vector[Node], left: Int, upStreams: Vector[Vector[ActorRef]]): Vector[Vector[ActorRef]] = {
    prevs match {
      case head +: tail =>
        gatherUpStreams(tail, left - 1, upStreams :+ head.getUpStreams)
      case _ =>
        upStreams
    }
  }

  override def initialize(sender: ActorRef): Unit = {
    val upStreams = gatherUpStreams(prevs, left, Vector())
    deployed.foreach(_.tell(MultiInitializer(upStreams), sender))
    prevs.foreach(_.initialize(sender))
  }

  override def restore(sender: ActorRef, uuid: String): Unit = {
    val upStreams = gatherUpStreams(prevs, left, Vector())
    deployed.foreach(_.tell(MultiRestoreSnapshot(uuid, upStreams), sender))
    prevs.foreach(_.restore(sender, uuid))
  }
}
