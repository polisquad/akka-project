package echo

import akka.actor.ActorSystem
import akka.testkit.{TestKit, TestProbe}
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike}

class EchoScalaTest
  extends TestKit(ActorSystem("EchoTest"))
  with FunSuiteLike
  with BeforeAndAfterAll
{

  test("echo reply") {
    val probe = TestProbe()
    val echoActor = system.actorOf(EchoScala.props())

    echoActor.tell(EchoScala.Msg("Hello, World!"), probe.ref)
    probe.expectMsg("Hello, World!")
  }

  override protected def afterAll(): Unit = system.terminate()
}
