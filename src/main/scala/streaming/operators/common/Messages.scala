package streaming.operators.common

import akka.actor.ActorRef

object Messages {
  // TODO place each message where it should belong
  // TODO separate like in graph package when refactoring operators???

  final case class Tuple[T](key: String, value: T, offset: Long) {
    override def hashCode(): Int = key.hashCode()
  }

  final case class Initializer(uuid: String, upStreams: Vector[ActorRef])

  final case class MultiInitializer(uuid: String, upStreams: Vector[Vector[ActorRef]])

  case object Initialized

  final case class RestoreSnapshot(uuid: String, upStreams: Vector[ActorRef])

  final case class MultiRestoreSnapshot(uuid: String, upStreams: Vector[Vector[ActorRef]])

  final case class RestoredSnapshot(uuid: String)

  final case class Marker(uuid: String, offset: Long)

  final case class TakeSnapshot(uuid: String)

  final case class SnapshotTaken(uuid: String)

  case object SnapshotFailed

  case object RestoreSnapshotFailed

  final case class MarkerAck(uuid: String)

  case object MarkersLost
}
