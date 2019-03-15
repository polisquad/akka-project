package streaming.graph

import akka.actor.{ActorContext, ActorRef}
import streaming.graph.nodes._
import streaming.graph.nodes.types.OneToOneNode

class GraphBuilder(stream: Stream)(implicit context: ActorContext) {
  import GraphBuilder._

  def initializeGraph(graphCreator: ActorRef): GraphInfo =
    deployGraph(graphCreator, Initialize)

  def restoreGraph(graphCreator: ActorRef, uuid: String): GraphInfo =
    deployGraph(graphCreator, Restore(uuid))

  private def deployGraph(graphCreator: ActorRef, deployMode: DeployMode): GraphInfo = {
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

    deployMode match {
      case Initialize =>
        initialize(graphCreator, source, sink)
      case Restore(uuid) =>
        restore(graphCreator, source, sink, uuid)
    }

  }

  private def initialize(graphCreator: ActorRef, source: SourceNode, sink: SinkNode): GraphInfo = {
    sink.initialize(graphCreator)
    val (operators, numDeployed) = parseGraph(stream)
    GraphInfo(source.deployed(0), operators, sink.deployed(0), numDeployed)
  }

  private def restore(graphCreator: ActorRef, source: SourceNode, sink: SinkNode, uuid: String): GraphInfo = {
    sink.restore(graphCreator, uuid)
    val (operators, numDeployed) = parseGraph(stream)
    GraphInfo(source.deployed(0), operators, sink.deployed(0), numDeployed)
  }

  private def parseGraph(graph: Stream): (Set[ActorRef], Int) = {
    var deployedCounts: Int = 0
    var nodes: Set[ActorRef] = Set()

    graph.nodes.foreach {
      case n: SplitNode =>
        deployedCounts += n.deployed.size
        nodes = nodes ++ n.deployed

        n.subStreams.foreach { subStream =>
          val (subNodes, subDeployedCounts) = parseGraph(subStream)
          deployedCounts += subDeployedCounts
          nodes = nodes ++ subNodes
        }
      case n =>
        deployedCounts += n.deployed.size
        nodes = nodes ++ n.deployed
    }
    (nodes, deployedCounts + 2) // + 2 because of sink and source
  }

}

object GraphBuilder {
  def apply(stream: Stream)(implicit context: ActorContext): GraphBuilder = new GraphBuilder(stream)

  final case class GraphInfo(source: ActorRef, operators: Set[ActorRef], sink: ActorRef, numDeployed: Int)

  sealed trait DeployMode
  case object Initialize extends DeployMode
  final case class Restore(uuid: String) extends DeployMode
}
