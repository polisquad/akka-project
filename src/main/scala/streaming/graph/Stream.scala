package streaming.graph

import streaming.graph.nodes._
import streaming.graph.nodes.OneToOneNode


class Stream(val nodes: Vector[Node]) {

  def addOneToOne(newNode: OneToOneNode): Stream = {
    nodes match {
      case _ :+ last =>
        newNode.prev = last
        Stream(nodes :+ newNode)
      case _ =>
        Stream(nodes :+ newNode)
    }
  }

  def map(parallelism: Int, f: (String, String) => (String, String)): Stream =
    addOneToOne(MapNode(parallelism, f))

  def flatMap(parallelism: Int, f: (String, String) => Seq[(String, String)]): Stream =
    addOneToOne(FlatMapNode(parallelism, f))

  def filter(parallelism: Int, f: (String, String) => Boolean): Stream =
    addOneToOne(FilterNode(parallelism, f))

  def aggregate(parallelism: Int, f: Seq[(String, String)] => (String, String), toAccumulate: Int): Stream =
    addOneToOne(AggregateNode(parallelism, f, toAccumulate))

  def splitThenMerge(subStreams: Seq[Stream], splitParallelism: Int, mergeParallelism: Int): Stream = {
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
        mergeNode.prevs :+ lastNode
    }

    Stream(nodes :+ splitNode :+ mergeNode)
  }



}

object Stream {
  def createFromDefaultSource(): Stream = new Stream(Vector())

  def apply(nodes: Vector[Node]): Stream = new Stream(nodes)
}
