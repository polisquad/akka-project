package streaming

import akka.actor.{Actor, ActorLogging}

class MergeOperator extends Actor with ActorLogging {
  // TODO multi upstream to one downstream
  override def receive: Receive = ???
}

object MergeOperator {

}
