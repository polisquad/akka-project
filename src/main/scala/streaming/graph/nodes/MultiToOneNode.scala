package streaming.graph.nodes

import akka.actor.{ActorContext, ActorRef}
import streaming.Streaming.{MultiInitializer}

import scala.annotation.tailrec

abstract class MultiToOneNode(parallelism: Int, multi: Int) extends Node(parallelism) {
  var prevs: Vector[Node] = Vector()
  val left: Int = multi

  def deploy(downStreams: Vector[ActorRef])(implicit context: ActorContext): Vector[ActorRef]

  override def backWard(downStreams: Vector[ActorRef])(implicit context: ActorContext): Unit = {
    deployed = deploy(downStreams)
    prevs.foreach { prev => prev.backWard(deployed) }
  }

  override def initialize(sender: ActorRef): Unit = {

    @tailrec
    def loop(prevs: Vector[Node], left: Int, upStreams: Vector[Vector[ActorRef]]): Vector[Vector[ActorRef]] = {
      prevs match {
        case head +: tail =>
          loop(tail, left - 1, upStreams :+ head.getUpStreams)
        case _ =>
          upStreams
      }
    }

    val upStreams = loop(prevs, left, Vector())
    deployed.foreach(_.tell(MultiInitializer(upStreams), sender))
    prevs.foreach(_.initialize(sender))
  }
}
