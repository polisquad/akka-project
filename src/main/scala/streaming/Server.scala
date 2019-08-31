package streaming

import java.util.concurrent.ConcurrentLinkedDeque

import akka.actor.{ActorRef, ActorSystem, Kill, PoisonPill}
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{Directives, Route}
import streaming.Engine.readGraph
import streaming.server.{JobDescription, JsonSupport}

import scala.collection.JavaConverters._
import scala.collection.immutable.HashMap
import scala.collection.mutable.ListBuffer
import scala.util.parsing.json.JSON

object Server extends Directives with JsonSupport {
  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem("server")
    implicit val materializer = ActorMaterializer()
    var jobs = new HashMap[JobDescription, ActorRef]

    lazy val routes: Route =
      pathPrefix("jobs") {
        get {
          var ls = new ConcurrentLinkedDeque[JobDescription]()
          jobs.foreach(job => {
            ls.add(job._1)
          })
          complete(ls.asScala)
        } ~
          post {
            entity(as[JobDescription]) { job =>
              val jobName = job.name
              val graphPath = job.path

              val graph = readGraph(graphPath)
              val newMasterNode: ActorRef = system.actorOf(MasterNode.props(() => graph), jobName)
              val jobDescription = JobDescription(jobName, graphPath)
              val result = jobs.filter(_._1.name == jobName)
              if (result.isEmpty) {
                jobs = jobs.updated(jobDescription, newMasterNode)
                newMasterNode ! MasterNode.CreateTopology
              } // do not start new job if it already exists

              complete(jobDescription)
            }
          } ~
          delete {
            pathEnd {
              complete("Specify job to kill")
            } ~
              path(Remaining) { name =>
                val result = jobs.filter(_._1.name == name)
                if (result.nonEmpty) {
                  val jobDescription = result.head._1
                  jobs = jobs - jobDescription
                  result.head._2 ! PoisonPill  // kill the job
                  complete {
                    jobDescription
                  }
                } else {
                  complete ("No jobs with name " + name + " found")
                }
              }
          }
      }

    Http().bindAndHandle(routes, "localhost", 8080)
  }
}
