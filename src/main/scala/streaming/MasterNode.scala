package streaming

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Actor, ActorLogging, ActorRef, AllForOneStrategy, Props, SupervisorStrategy}
import streaming.Source.{InitializeSource, Produce}
import streaming.Streaming.Initializer

import scala.concurrent.duration._

class MasterNode extends Actor with ActorLogging {
  import MasterNode._

  var toInitialize: Int = 0
  var source: ActorRef = _

  override def receive: Receive = {
    case CreateTopology => createTopology()

    case InitializedAck =>
      toInitialize -= 1
      if (toInitialize == 0)
        self ! StartJob

    case StartJob =>
      source ! Produce
  }

  override def supervisorStrategy: SupervisorStrategy = {
    AllForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 10 seconds) {
      case _ => Restart
    }
  }

  def createTopology(): Unit = {
    val sink = context.actorOf(Sink.props, "Sink")

    val map21 = context.actorOf(MapOperator.props((s1, s2) => (s1, s2 + "!!!"), Vector(sink)), "Map21")
    val map22 = context.actorOf(MapOperator.props((s1, s2) => (s1, s2 + "!!!"), Vector(sink)), "Map22")

    val sinkInitializer = Initializer(Vector(map21, map22))
    sink ! sinkInitializer

    val map11 = context.actorOf(MapOperator.props((s1, s2) => (s1, s2.toLowerCase()), Vector(map21, map22)), "Map11")
    val map12 = context.actorOf(MapOperator.props((s1, s2) => (s1, s2.toLowerCase()), Vector(map21, map22)), "Map12")

    val map2Initializer = Initializer(Vector(map11, map12))
    map21 ! map2Initializer
    map22 ! map2Initializer

    source = context.actorOf(Source.props(Vector(map11, map12)), "Source")
    source ! InitializeSource

    val map1Initializer = Initializer(Vector(source))
    map11 ! map1Initializer
    map12 ! map1Initializer

    toInitialize = 6
  }
}

object MasterNode {
  def props: Props = Props(new MasterNode)

  case object CreateTopology
  case object InitializedAck
  case object StartJob
}
