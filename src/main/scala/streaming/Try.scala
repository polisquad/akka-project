package streaming

import akka.actor.ActorSystem

object Try {

  def main(args: Array[String]): Unit = {
    val system = ActorSystem("prova")

    val masterNode = system.actorOf(MasterNode.props)

    masterNode ! MasterNode.CreateTopology

  }

}
