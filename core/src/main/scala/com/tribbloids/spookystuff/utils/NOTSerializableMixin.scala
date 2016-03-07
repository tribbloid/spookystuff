package com.tribbloids.spookystuff.utils

import java.io.NotSerializableException

/**
  * Any subclass in the closure cleaned by Spark ClosureCleaner should trigger a runtime error.
  */
trait NOTSerializableMixin extends Serializable {

  def writeObject(out: java.io.ObjectOutputStream): Unit = throw new NotSerializableException()
  def readObject(in: java.io.ObjectInputStream): Unit = throw new NotSerializableException()
  def readObjectNoData(): Unit = throw new NotSerializableException()
}
