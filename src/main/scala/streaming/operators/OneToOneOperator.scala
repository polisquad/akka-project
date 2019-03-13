package streaming.operators

import akka.actor.{Actor, ActorLogging, ActorRef, Stash, Timers}

abstract class OneToOneOperator(downStreams: Vector[ActorRef]) extends Operator {

  def init(upStreams: Vector[ActorRef]): Unit = {
    upOffsets = upStreams.map(x => x -> 0L).toMap
    downOffsets = downStreams.map(x => x -> 0L).toMap
    blockedChannels = upStreams.map(x => x -> false).toMap
  }





}

