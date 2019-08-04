package streaming.operators.common

import java.io.{FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}

/**
  * Utils to write and read operator state
  */
object State {

  val basePath = "/tmp/state/"

  def writeLong(offset: Long, fileName: String): Unit = {
    val offsetState = new ObjectOutputStream(new FileOutputStream(basePath + fileName))
    offsetState.writeLong(offset)
    offsetState.close()
  }

  def readLong(fileName: String): Long = {
    val offsetState = new ObjectInputStream(new FileInputStream(basePath + fileName))
    val long = offsetState.readLong()
    offsetState.close()
    long
  }

  def writeVector[T](vector: Vector[T], fileName: String): Unit = {
    val offsetState = new ObjectOutputStream(new FileOutputStream(basePath + fileName))
    offsetState.writeObject(vector)
    offsetState.close()
  }

  def readVector[T](fileName: String): Vector[T] = {
    val offsetState = new ObjectInputStream(new FileInputStream(basePath + fileName))
    val vec = offsetState.readObject()
    offsetState.close()
    vec.asInstanceOf[Vector[T]]
  }

}
