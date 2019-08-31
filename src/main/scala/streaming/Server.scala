package streaming

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route

object Server {
  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem("server")
    implicit val materializer = ActorMaterializer()

    lazy val routes: Route = get { // `get` for HTTP GET method
      complete("Hello World")
    }

    Http().bindAndHandle(routes, "localhost", 8080)

  }
}
