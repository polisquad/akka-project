package streaming

import akka.actor.ActorRef

object Streaming {
  // TODO place each message where it should belong

  final case class Initializer(upStreams: Vector[ActorRef])

  case object Initialized

  final case class InitializeException(msg: String) extends Exception(msg)

  final case class Marker(uuid: String, offset: Long)

  final case class SnapshotTaken(uuid: String)
  case object SnapshotFailed
  case object MarkerAck

  case object MarkersLost
}
