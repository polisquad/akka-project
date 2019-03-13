package streaming

import akka.actor.ActorSystem
import streaming.graph.Stream

object Try {

  def someStream(): Stream =
    Stream.createFromDefaultSource()
      .map(parallelism = 2, (s1, s2) => {
        val x = new scala.util.Random().nextFloat()
        println(x)
              if (x > 0.9) {
                (s1, s2 + "!!!")
              } else {
                throw new Exception("Failed!")
              }
            })
      .map(parallelism = 2, (s1, s2) => (s1, s2 + "!!!"))

  def main(args: Array[String]): Unit = {

    val userStream = someStream()

    val system = ActorSystem("prova")
    val masterNode = system.actorOf(MasterNode.props(someStream), "JobMaster")

    masterNode ! MasterNode.CreateTopology
  }

}
