package streaming.operators

import akka.actor.{Actor, ActorLogging, ActorRef, Stash, Timers}

abstract class Operator extends Actor with ActorLogging with Stash with Timers {
  var upOffsets: Map[ActorRef, Long] = _
  var downOffsets: Map[ActorRef, Long] = _

  var blockedChannels: Map[ActorRef, Boolean] = _
  var numBlocked: Int = 0

  var markersToAck: Int = _
  var uuidToAck: String = _

  def snapshot(): Unit =
  // TODO
    log.info("Snapshotting...")

  def restoreSnapshot(uuid: String): Unit =
  // TODO
    log.info(s"Restoring snapshot ${uuid}...")

  override def receive: Receive = {
    initializeBehavior orElse restoreBehavior
  }

  def initializeBehavior: Receive
  def restoreBehavior: Receive



}
