package streaming.operators.types

import akka.actor.{Actor, ActorLogging, Stash, Timers}

abstract class Operator extends Actor with ActorLogging with Stash with Timers