package example

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory

object WorkerMachine {

  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load("example/worker_machine.conf")
    val system = ActorSystem("WorkerMachine", config)
  }

}