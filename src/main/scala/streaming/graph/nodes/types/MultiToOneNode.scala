package streaming.graph.nodes.types

import akka.actor.{ActorContext, ActorRef}
import streaming.operators.common.Messages.{MultiInitializer, MultiRestoreSnapshot}

import scala.annotation.tailrec

abstract class MultiToOneNode(parallelism: Int, multi: Int) extends Node(parallelism) {
  private var _prevs: Vector[Node] = Vector()
  private val left: Int = multi

  protected def deploy(downStreams: Vector[ActorRef])(implicit context: ActorContext): Vector[ActorRef]

  override def backWard(downStreams: Vector[ActorRef])(implicit context: ActorContext): Unit = {
    deployed = deploy(downStreams)
    prevs.foreach { prev => prev.backWard(deployed) }
  }

  @tailrec private def gatherUpStreams(inPrevs: Vector[Node], left: Int, upStreams: Vector[Vector[ActorRef]]): Vector[Vector[ActorRef]] = {
    inPrevs match {
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

  def prevs: Vector[Node] = _prevs
  def prevs_=(prevs: Vector[Node]): Unit = _prevs = prevs
}
