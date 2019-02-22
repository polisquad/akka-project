package streaming

import akka.actor.ActorRef

object Streaming {

  final case class Initializer(upStreams: Vector[ActorRef])

}
