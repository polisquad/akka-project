package streaming.operators

import java.io.RandomAccessFile

import akka.actor.Props
import streaming.operators.common.Messages.Tuple
import streaming.operators.common.State
import streaming.operators.types.OneToZeroOperator

// Text source operator
class SinkOperator[I](sink: String) extends OneToZeroOperator[I] {

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

  override def writeResult(t: Tuple[I]): Unit = {
    upOffsets = upOffsets.updated(sender(), t.offset + 1)
    writeToFile(t.value)
    log.info(s"Written result: ${t.value}")
  }

  def writeToFile(value: I): Unit = {
    if (resultFile == null) {
      // There is no result file yet
      resultFile = new RandomAccessFile(sink, "rw")
    }

    val strValue = value.toString()

    resultFile.seek(filePointer)
    resultFile.writeBytes(strValue)
    resultFile.writeBytes(System.lineSeparator())

    filePointer += strValue.length() + SeparatorLength
  }

}


object SinkOperator {
  def props(sink: String): Props = Props(new SinkOperator(sink))
}