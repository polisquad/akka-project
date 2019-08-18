package streaming

import akka.actor.{ActorRef, ActorSystem}
import streaming.graph.Graph

object Try {

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
      .fromFileSource("/Users/lpraat/develop/akka-project/prova.txt")
      .splitThenMerge(Vector(addExclamationPoints, addQuestionMarks), 2, 2)
      .toSink("/Users/lpraat/develop/akka-project/results.txt")
     // .filter(2, (key, value) => value.length() < 10)
  }

  def main(args: Array[String]): Unit = {
    val system = ActorSystem("prova")
    val masterNode: ActorRef = system.actorOf(MasterNode.props(someGraph), "JobMaster")


    masterNode ! MasterNode.CreateTopology
  }

}
