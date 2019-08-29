package streaming

import java.io.{FileInputStream, ObjectInputStream}
import akka.actor.{ActorRef, ActorSystem}
import streaming.graph.Graph

import scala.collection.immutable.HashMap

object Engine {

  def main(args: Array[String]): Unit = {
    var jobs = new HashMap[String, ActorRef]
    val system = ActorSystem("engine")

    while (true) {
      println("Enter a command")

      try {
        val command = io.StdIn.readLine()

        if (command == "submit") {
          println("Enter the path of the graph")
          val graphPath = io.StdIn.readLine()

          println("Insert a name for the Job")
          val jobName = io.StdIn.readLine()

          val graph = readGraph(graphPath)

          val newMasterNode: ActorRef = system.actorOf(MasterNode.props(() => graph), jobName)

          jobs = jobs.updated(jobName, newMasterNode)
          newMasterNode ! MasterNode.CreateTopology

          println(f"Started Job: ${jobName}")
        } else if (command == "list") {

          if (jobs.nonEmpty) {
            println("Active jobs are: ")

            jobs.foreach(job => {
              println(f"Job: ${job._1}")
            })

          } else {

            println("There are no active jobs")
          }
        } else if (command == "kill") {

        }

      } catch {
        case t: Throwable  =>
          println(t)
          println("Failed, restarting...")
      }

    }
  }

  def readGraph(graphPath: String): Graph = {
    val graphInStream = new ObjectInputStream(new FileInputStream(graphPath))
    val graph = graphInStream.readObject().asInstanceOf[Graph]
    graphInStream.close()
    graph
  }

}
