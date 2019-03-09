package streaming

import akka.actor.ActorRef

object Streaming {
  // TODO place each message where it should belong

  final case class Initializer(upStreams: Vector[ActorRef])

  final case class MultiInitializer(upStreams: Vector[Vector[ActorRef]])

  case object Initialized


  //final case class InitializerFromSnapshot(uuid: String, upStreams: Vector[ActorRef])

  //final case class InitializedFromSnapshot(uuid: String)

  final case class RestoreSnapshot(uuid: String, upStreams: Vector[ActorRef])

  final case class MultiRestoreSnapshot(uuid: String, upStreams: Vector[Vector[ActorRef]])

  final case class RestoredSnapshot(uuid: String)

  final case class Marker(uuid: String, offset: Long)

  final case class SnapshotTaken(uuid: String)

  case object SnapshotFailed

  case object RestoreSnapshotFailed

  final case class MarkerAck(uuid: String)

  case object MarkersLost
}
