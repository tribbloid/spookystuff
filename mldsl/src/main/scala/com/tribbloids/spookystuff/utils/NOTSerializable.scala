package com.tribbloids.spookystuff.utils

import java.io.NotSerializableException

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}

/**
  * Any subclass in the closure cleaned by Spark ClosureCleaner will trigger a runtime error.
  */
trait NOTSerializable extends Serializable with KryoSerializable {

  lazy val error = new NotSerializableException(s"${this.getClass.getCanonicalName} is NOT serializable")

  def writeObject(out: java.io.ObjectOutputStream): Unit =
    throw error

  def readObject(in: java.io.ObjectInputStream): Unit =
    throw error

  def readObjectNoData(): Unit =
    throw error

  override def write(kryo: Kryo, output: Output): Unit =
    throw error

  override def read(kryo: Kryo, input: Input): Unit =
    throw error
}
