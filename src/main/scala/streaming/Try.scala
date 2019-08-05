package streaming

import akka.actor.ActorSystem
import streaming.graph.Graph

object Try {

  def someStream(): Graph = {

    val addQuestionMarks: Graph =
      Graph
        .createFromDefaultSource()
        .map(2, (s1, s2) => {
          val x = new scala.util.Random().nextFloat()
          if (x > 0.5) {
            (s1, s2 + "???")
          } else {
            throw new Exception("Failed!")
          }
        })

    val addExclamationPoints: Graph =
      Graph.
        createFromDefaultSource()
        .map(2, (s1, s2) => (s1, s2 + "!!!"))


    Graph
      .createFromDefaultSource()
     // .filter(2, (s1, s2) => s1 != "z")
      .splitThenMerge(Vector(addExclamationPoints, addQuestionMarks), 4, 2)
      .map(5, (s1, s2) => (s1, s2))
  }

  def main(args: Array[String]): Unit = {
    val system = ActorSystem("prova")
    val masterNode = system.actorOf(MasterNode.props(someStream), "JobMaster")

    masterNode ! MasterNode.CreateTopology
  }

}
