package streaming.operators

import java.io.RandomAccessFile

import akka.actor.{ActorRef, Props}
import streaming.Config
import streaming.operators.common.State
import streaming.operators.types.ZeroToOneOperator


// Text source operator
class SourceOperator(downStreams: Vector[ActorRef], source: String) extends ZeroToOneOperator[String](downStreams) {

  var data: RandomAccessFile = _
  var offset: Long = 0

  // Get the separator length on the OS on which the source is running
  val SeparatorLength = System.lineSeparator().length()

  override def snapshot(uuid: String): Unit = {
    log.info(s"Snapshotting ${uuid}...")

    State.writeLong(offset, uuid + "source-offset.txt")

    log.info(f"Written to state offset: ${offset}")
  }

  override def restoreSnapshot(uuid: String): Unit = {
    log.info(s"Restoring snapshot ${uuid}...")

    offset = State.readLong(uuid + "source-offset.txt")

    log.info(f"Restored offset: ${offset}")
  }

  override def readNext(): Option[(String, String)] = {
    if (data == null) {
      data = new RandomAccessFile(source, "r")
    }

    if (offset < data.length()) {
      data.seek(offset)
      val line = data.readLine()
      val lineSplit = line.split(Config.KeyValueSeparator)

      offset += line.length + SeparatorLength

      Some((lineSplit(0), lineSplit(1)))
    } else {
      None
    }
  }

  override def pause(): Unit = {
    data.close()
    data = null
  }

}

object SourceOperator {
  def props(downStreams: Vector[ActorRef], source: String): Props = Props(new SourceOperator(downStreams, source))
}