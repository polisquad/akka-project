package streaming.graph

import akka.actor.{ActorContext, ActorRef}
import streaming.graph.nodes._

class GraphBuilder(stream: Stream)(implicit context: ActorContext) {

  case class GraphInfo(source: ActorRef, operators: Set[ActorRef], sink: ActorRef, numDeployed: Int)

  def deployGraph(graphCreator: ActorRef): GraphInfo = {
    val source = SourceNode()
    val sink = SinkNode()

    val firstNode = stream.nodes.head
    val lastNode = stream.nodes.last

    firstNode match {
      case n: OneToOneNode =>
        n.prev = source
      case _ =>
        throw new Exception("First node must be a OneToOneNode")
    }

    sink.prev = lastNode

    sink.backWard(Vector())

    // TODO if deploy else restore
    sink.initialize(graphCreator)

    val (operators, numDeployed) = parseGraph(stream)

    GraphInfo(source.deployed(0), operators, sink.deployed(0), numDeployed)
  }

  def parseGraph(graph: Stream): (Set[ActorRef], Int) = {

    var deployedCounts: Int = 0
    var nodes: Set[ActorRef] = Set()

    graph.nodes.foreach {
      case n: SplitNode =>
        deployedCounts += n.deployed.size
        nodes = nodes ++ n.deployed

        n.subStreams.foreach { substream =>
          val (subNodes, subDeployedCounts) = parseGraph(substream)
          deployedCounts += subDeployedCounts
          nodes = nodes ++ subNodes
        }
      case n =>
        deployedCounts += n.deployed.size
        nodes = nodes ++ n.deployed
    }

    (nodes, deployedCounts)
  }

}

object GraphBuilder {
  def apply(stream: Stream)(implicit context: ActorContext): GraphBuilder = new GraphBuilder(stream)
}
