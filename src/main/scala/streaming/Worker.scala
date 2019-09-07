package streaming

import com.typesafe.config.ConfigFactory
import akka.actor.ActorSystem

object WorkerMachine {

  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load("worker_machine.conf")
    val system = ActorSystem("WorkerMachine", config)
  }

}