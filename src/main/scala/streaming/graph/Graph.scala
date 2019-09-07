package streaming.graph

import streaming.graph.nodes._
import streaming.graph.nodes.types.{Node, OneToMultiNode, OneToOneNode}


class Graph(val nodes: Vector[Node], val sourceWithAddress: (String, String), val sinkWithAddress: (String, String)) extends Serializable {

  private def addOneToOne(newNode: OneToOneNode): Graph = {
    nodes match {
      case _ :+ last =>
        newNode.prev = last
        Graph(nodes :+ newNode, sourceWithAddress, sinkWithAddress)
      case _ =>
        Graph(nodes :+ newNode, sourceWithAddress, sinkWithAddress)
    }
  }

  def map[I,O](parallelism: Int, f: (String, I) => (String, O), address: String): Graph = {
    addOneToOne(MapNode[I,O](parallelism, f, address))
  }

  def flatMap[I,O](parallelism: Int, f: (String, I) => Seq[(String, O)], address: String): Graph = {
    addOneToOne(FlatMapNode[I,O](parallelism, f, address))
  }

  def filter[I](parallelism: Int, f: (String, I) => Boolean, address: String): Graph = {
    addOneToOne(FilterNode[I](parallelism, f, address))
  }

  def aggregate[I,O](parallelism: Int, f: Seq[(String, I)] => (String, O), toAccumulate: Int, address: String): Graph = {
    addOneToOne(AggregateNode[I,O](parallelism, f, toAccumulate, address))
  }

  def splitThenMerge[I,O](subStreams: Seq[Graph], splitParallelism: Int, splitAddress: String, mergeParallelism: Int, mergeAddress: String): Graph = {
    val numOfSplit = subStreams.size
    val splitNode = SplitNode[I](splitParallelism, numOfSplit, subStreams, splitAddress)
    val mergeNode = MergeNode[O](mergeParallelism, numOfSplit, mergeAddress)

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

    Graph(nodes :+ splitNode :+ mergeNode, sourceWithAddress, sinkWithAddress)
  }

  def toSink(sink: String, address: String): Graph = {
    Graph(nodes, sourceWithAddress, (sink, address))
  }

}

object Graph {

  def createSubGraph(): Graph = new Graph(Vector(), ("", ""), ("", ""))

  def fromFileSource(fileName: String, address: String): Graph = new Graph(Vector(), (fileName, address), ("", ""))

  def apply(nodes: Vector[Node], sourceWithAddress: (String, String), sinkWithAddress: (String, String)): Graph =
    new Graph(nodes, sourceWithAddress, sinkWithAddress)
}
