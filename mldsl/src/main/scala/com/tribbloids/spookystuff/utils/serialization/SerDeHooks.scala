package com.tribbloids.spookystuff.utils.serialization

import java.io.{IOException, ObjectInputStream, ObjectOutputStream}

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.serializers.DefaultSerializers.KryoSerializableSerializer
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import com.tribbloids.spookystuff.utils.CommonUtils

/**
  *
  */
final case class SerDeHooks(
    beforeWrite: () => Unit = () => {},
    beforeRead: () => Unit = () => {}
) extends Serializable
    with KryoSerializable {

  @throws(classOf[IOException])
  private def writeObject(out: ObjectOutputStream): Unit = {
    beforeWrite()
    out.defaultWriteObject()
  }

  @throws(classOf[IOException])
  private def readObject(in: ObjectInputStream): Unit = {
    beforeRead()
    in.defaultReadObject()
  }
  @throws(classOf[IOException])
  private def readObjectNoData(): Unit = {
    beforeRead()
  }

  override def write(kryo: Kryo, output: Output): Unit = {
    beforeWrite()
//    new KryoSerializableSerializer().write(kryo, output, this)
//    kryo.writeClassAndObject(output, this)
  }

  override def read(kryo: Kryo, input: Input): Unit = {
    beforeRead()
  }
}
