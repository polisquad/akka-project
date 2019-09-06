package streaming

import akka.actor.{ActorRef, ActorSystem}
import streaming.graph.Graph

object Try {

  case class Point(x: Float, y: Float) {
    override def toString:String = f"$x,$y"
  }

  def someGraph(): Graph = {
    def squarePoint: Graph = Graph
      .createSubGraph()
      .map(2, (key, value: Point) => (key, Point(value.x*value.x, value.y*value.y)))

    def halvePoint: Graph = Graph
      .createSubGraph()
      .map(2, (key, value: Point) => (key, Point(value.x/2, value.y/2)))


    Graph
      .fromFileSource("./in.txt")
      .map(2, (key, value: String) => {
        val dimensions = value.split(",")
        (key, Point(dimensions(0).toFloat, dimensions(1).toFloat))
      })
      .splitThenMerge(Vector(squarePoint, halvePoint), 2, 2)
      .toSink("./out.txt")
  }


  def main(args: Array[String]): Unit = {
    val system = ActorSystem("prova")
    val masterNode: ActorRef = system.actorOf(MasterNode.props(someGraph), "JobMaster")


    masterNode ! MasterNode.CreateTopology
  }
}