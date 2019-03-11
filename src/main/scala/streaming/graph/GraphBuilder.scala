package streaming.graph

import akka.actor.{ActorContext, ActorRef}
import streaming.graph.nodes._

class GraphBuilder(stream: Stream)(implicit context: ActorContext) {

  def deployGraph(graphCreator: ActorRef): Unit = {
    val source = SourceNode()
    val sink = SinkNode()

    val firstNode = stream.nodes.head
    val lastNode = stream.nodes.last

    firstNode match {
      case n: OneToOneNode =>
        n.prev = firstNode
      case _ =>
        throw new Exception("First node must be a OneToOneNode")
    }

    sink.prev = lastNode

    sink.backWard(Vector())

    // TODO if deploy else restore
    sink.initialize(graphCreator)

    val fullGraph = source +: stream.nodes :+ sink

    // TODO add full graph
    // fullGraph.map {
    //  case n: SplitNode =>

    //}
  }
}

object GraphBuilder {
  def apply(stream: Stream)(implicit context: ActorContext): GraphBuilder = new GraphBuilder(stream)
}
