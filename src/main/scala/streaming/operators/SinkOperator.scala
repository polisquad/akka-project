package streaming.operators

import akka.actor.Props
import streaming.operators.types.OneToZeroOperator
import java.io.RandomAccessFile
import streaming.operators.common.State
import streaming.operators.common.Messages.Tuple

// Text source operator
class SinkOperator(sink: String) extends OneToZeroOperator {

  var resultFile: RandomAccessFile = _
  var filePointer: Long = 0

  // Get the separator length on the OS on which the sink is running
  val SeparatorLength = System.lineSeparator().length()

  override def snapshot(uuid: String): Unit = {
    log.info(s"Snapshotting ${uuid}...")

    State.writeLong(filePointer, uuid + "sink-pointer.txt")

    log.info(f"Written to state file pointer: ${filePointer}")
  }

  override def restoreSnapshot(uuid: String): Unit = {
    log.info(s"Restoring snapshot ${uuid}...")

    filePointer = State.readLong(uuid + "sink-pointer.txt")

    log.info(f"Restored file pointer: ${filePointer}")
  }

  override def writeResult(t: Tuple): Unit = {
    upOffsets = upOffsets.updated(sender(), t.offset + 1)
    writeToFile(t.value)
    log.info(s"Written result: ${t.value}")
  }

  def writeToFile(value: String): Unit = {
    if (resultFile == null) {
      // There is no result file yet
      resultFile = new RandomAccessFile(sink, "rw")
    }

    resultFile.seek(filePointer)
    resultFile.writeBytes(value)
    resultFile.writeBytes(System.lineSeparator())

    filePointer += value.length + SeparatorLength
  }

}


object SinkOperator {
  def props(sink: String): Props = Props(new SinkOperator(sink))
}