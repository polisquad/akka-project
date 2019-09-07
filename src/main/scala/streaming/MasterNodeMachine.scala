package streaming

import akka.actor.{ActorRef, ActorSystem}
import streaming.graph.Graph
import com.typesafe.config.ConfigFactory
import scala.util.Random

object MasterNodeMachine {

  case class Point(x: Float, y: Float) {
    override def toString:String = f"$x,$y"
  }

  def someGraph(): Graph = {

    val masterNodeMachineAddress = "akka.tcp://MasterNodeMachine@127.0.0.1:7777"
    val workerMachineAddress = "akka.tcp://WorkerMachine@127.0.0.1:2250"

    def squarePoint: Graph = Graph
      .createSubGraph()
      .map(parallelism = 2, (key, value: Point) => (key, Point(value.x*value.x, value.y*value.y)), workerMachineAddress)

    def halvePoint: Graph = Graph
      .createSubGraph()
      .map(parallelism = 2, (key, value: Point) => (key, Point(value.x/2, value.y/2)), workerMachineAddress)


    Graph
      .fromFileSource("./in.txt", masterNodeMachineAddress)
      .map(2, (key, value: String) => {
        val dimensions = value.split(",")
        (key, Point(dimensions(0).toFloat, dimensions(1).toFloat))
      }, masterNodeMachineAddress)
      .splitThenMerge(
        Vector(squarePoint, halvePoint),
        splitParallelism = 2, splitAddress = masterNodeMachineAddress,
        mergeParallelism = 2, mergeAddress = masterNodeMachineAddress)
      .toSink("./out.txt", masterNodeMachineAddress)
  }

  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load("master_node_machine.conf")
    val system = ActorSystem("MasterNodeMachine", config)

    val masterNode: ActorRef = system.actorOf(MasterNode.props(someGraph), "JobMaster")
    masterNode ! MasterNode.CreateTopology
  }
}