package streaming

import akka.actor.ActorSystem
import streaming.graph.Stream

object Try {

  def someStream(): Stream =
    Stream()
      .map(parallelism = 2, (s1, s2) => (s1, s2.toLowerCase()))
      .map(parallelism = 2, (s1, s2) => (s1, s2 + "!!!"))

  def main(args: Array[String]): Unit = {

    val userStream = someStream()

    val system = ActorSystem("prova")
    val masterNode = system.actorOf(MasterNode.props, "JobMaster")

    masterNode ! MasterNode.CreateTopology(userStream)
  }

}
