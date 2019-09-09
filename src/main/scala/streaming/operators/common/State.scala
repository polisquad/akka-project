package streaming.operators.common

import java.io._

import streaming.Config

/**
  * Utils to write and read operator state
  */
object State {

  def writeLong(offset: Long, fileName: String): Unit = {
    val offsetState = new ObjectOutputStream(new FileOutputStream(new File(Config.StatePath + fileName)))
    offsetState.writeLong(offset)
    offsetState.close()
  }

  def readLong(fileName: String): Long = {
    val offsetState = new ObjectInputStream(new FileInputStream(Config.StatePath + fileName))
    val long = offsetState.readLong()
    offsetState.close()
    long
  }

  def writeVector[T](vector: Vector[T], fileName: String): Unit = {
    val offsetState = new ObjectOutputStream(new FileOutputStream(Config.StatePath + fileName))
    offsetState.writeObject(vector)
    offsetState.close()
  }

  def readVector[T](fileName: String): Vector[T] = {
    val offsetState = new ObjectInputStream(new FileInputStream(Config.StatePath + fileName))
    val vec = offsetState.readObject()
    offsetState.close()
    vec.asInstanceOf[Vector[T]]
  }

}
