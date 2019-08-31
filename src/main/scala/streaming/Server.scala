package streaming

import java.util.concurrent.ConcurrentLinkedDeque

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{Directives, Route}
import streaming.server.{JobDescription, JsonSupport}
import scala.collection.JavaConverters._
import scala.collection.immutable.HashMap
import scala.collection.mutable.ListBuffer
import scala.util.parsing.json.JSON

object Server extends Directives with JsonSupport{
  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem("server")
    implicit val materializer = ActorMaterializer()
    var jobs = new HashMap[String, ActorRef]

    lazy val routes: Route =
      path("jobs") {
        get {
          var ls = new ConcurrentLinkedDeque[JobDescription]()
            jobs.foreach(job => {
              ls.add(JobDescription(f"Job: ${job._1}"))
            })
          complete(ls.asScala)
        } ~
        post {
          complete("Post endpoint")
        } ~
        delete {
          complete("Delete endpoint")
        }
      }

    Http().bindAndHandle(routes, "localhost", 8080)
  }
}
