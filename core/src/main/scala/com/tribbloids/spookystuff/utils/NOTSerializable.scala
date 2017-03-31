package com.tribbloids.spookystuff.utils

import java.io.NotSerializableException

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}

/**
  * Any subclass in the closure cleaned by Spark ClosureCleaner will trigger a runtime error.
  */
trait NOTSerializable extends Serializable with KryoSerializable{

  def writeObject(out: java.io.ObjectOutputStream): Unit = throw new NotSerializableException()
  def readObject(in: java.io.ObjectInputStream): Unit = throw new NotSerializableException()
  def readObjectNoData(): Unit = throw new NotSerializableException()

  override def write (kryo: Kryo, output: Output): Unit = {
    throw new NotSerializableException()
  }

  override def read (kryo: Kryo, input: Input): Unit = {
    throw new NotSerializableException()
  }
}
