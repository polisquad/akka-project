package streaming

import java.io.{FileOutputStream, ObjectOutputStream}

import streaming.graph.Graph

object CreateGraph {

  def someGraph(): Graph = {
    val addQuestionMarks: Graph =
      Graph
        .createFromDefaultSource()
        .map(parallelism = 2, (s1, s2) => (s1, s2 + "???"))

    val addExclamationPoints: Graph =
      Graph
        .createFromDefaultSource()
        .map(2, (key, value) => (key, value + "!!!"))

    Graph
      .fromFileSource("/Users/lpraat/develop/akka-project/input2.txt")
      .splitThenMerge(Vector(addExclamationPoints, addQuestionMarks), 2, 2)
      .toSink("/Users/lpraat/develop/akka-project/outputNuovo.txt")
  }

  def main(args: Array[String]): Unit = {
    val g = someGraph()
    val graphOutStream = new ObjectOutputStream(new FileOutputStream("./2.graph"))
    graphOutStream.writeObject(g)
    graphOutStream.close()
  }

}