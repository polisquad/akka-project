package streaming.server

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import spray.json.DefaultJsonProtocol

case class JobDescription(name: String)

trait JsonSupport extends SprayJsonSupport with DefaultJsonProtocol {

  implicit val data = jsonFormat1(JobDescription)
}