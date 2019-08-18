package streaming.graph

import streaming.graph.nodes._
import streaming.graph.nodes.types.{Node, OneToMultiNode, OneToOneNode}


class Graph(val nodes: Vector[Node], val source: String, val sink: String) extends Serializable {

  private def addOneToOne(newNode: OneToOneNode): Graph = {
    nodes match {
      case _ :+ last =>
        newNode.prev = last
        Graph(nodes :+ newNode, source, sink)
      case _ =>
        Graph(nodes :+ newNode, source, sink)
    }
  }

  def map(parallelism: Int, f: (String, String) => (String, String)): Graph = {
    addOneToOne(MapNode(parallelism, f))
  }

  def flatMap(parallelism: Int, f: (String, String) => Seq[(String, String)]): Graph = {
    addOneToOne(FlatMapNode(parallelism, f))
  }

  def filter(parallelism: Int, f: (String, String) => Boolean): Graph = {
    addOneToOne(FilterNode(parallelism, f))
  }

  def aggregate(parallelism: Int, f: Seq[(String, String)] => (String, String), toAccumulate: Int): Graph = {
    addOneToOne(AggregateNode(parallelism, f, toAccumulate))
  }

  def splitThenMerge(subStreams: Seq[Graph], splitParallelism: Int, mergeParallelism: Int): Graph = {
    val numOfSplit = subStreams.size
    val splitNode = SplitNode(splitParallelism, numOfSplit, subStreams)
    val mergeNode = MergeNode(mergeParallelism, numOfSplit)

    subStreams.foreach {
      subStream =>
        val firstNode = subStream.nodes.head
        firstNode match {
          case n: OneToOneNode =>
            n.prev = splitNode
          case n: OneToMultiNode => // Ideally here one can chain split ops, does it work? //TODO
            n.prev = splitNode
          case _ =>
            throw new Exception("Graph is malformed")
        }

        val lastNode = subStream.nodes.last
        mergeNode.prevs = mergeNode.prevs :+ lastNode
    }

    if (nodes.nonEmpty) {
      splitNode.prev = nodes.last
    }

    Graph(nodes :+ splitNode :+ mergeNode, source, sink)
  }

  def toSink(sink: String): Graph = {
    Graph(nodes, source, sink)
  }

}

object Graph {
  def createFromDefaultSource(): Graph = new Graph(Vector(), "", "")

  def fromFileSource(fileName: String): Graph = new Graph(Vector(), fileName, "")

  def apply(nodes: Vector[Node], source: String, sink: String): Graph = new Graph(nodes, source, sink)
}
