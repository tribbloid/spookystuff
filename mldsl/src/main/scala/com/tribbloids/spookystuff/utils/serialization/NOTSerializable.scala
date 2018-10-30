package com.tribbloids.spookystuff.utils.serialization
import java.io.NotSerializableException

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}

/**
  * Any subclass in the closure cleaned by Spark ClosureCleaner will trigger a runtime error.
  */
trait NOTSerializable extends KryoSerializable {

  lazy val error = new NotSerializableException(s"${this.getClass.getCanonicalName} is NOT serializable")

  private val hooks = SerDeHook({ _ =>
    throw error
  }, { _ =>
    throw error
  })

  override def write(kryo: Kryo, output: Output): Unit =
    throw error

  override def read(kryo: Kryo, input: Input): Unit =
    throw error
}
