package echo

import akka.actor.{Actor, Props}

class EchoScala extends Actor {
  import EchoScala._

  override def receive: Receive = {
    case Msg(s) => sender() ! s
  }
}

object EchoScala {
  def props(): Props = Props(new EchoScala)

  final case class Msg(s: String)
}
