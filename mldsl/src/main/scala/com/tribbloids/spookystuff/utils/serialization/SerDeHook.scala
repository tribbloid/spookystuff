package com.tribbloids.spookystuff.utils.serialization

import java.io._

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}

/**
  * Attach as a member/property of another class to trigger when the other class is processed by SerDe
  * CAUTION: writeObject & readObject won't be effective unless declared private
  * as a result this class is declared final
  */
final case class SerDeHook(
    var beforeWrite: OutputStream => Any = _ => {},
    var afterRead: InputStream => Any = _ => {}
    //TODO: afterWrite?
) extends Serializable
    with KryoSerializable {

  @throws(classOf[IOException])
  private def writeObject(out: ObjectOutputStream): Unit = {
    beforeWrite(out)
    out.defaultWriteObject()
  }

  @throws(classOf[IOException])
  private def readObject(in: ObjectInputStream): Unit = {
    in.defaultReadObject()
    afterRead(in)
  }
  @throws(classOf[IOException])
  private def readObjectNoData(): Unit = {
    afterRead(new ByteArrayInputStream(Array.empty[Byte]))
  }

  override def write(kryo: Kryo, output: Output): Unit = {
    beforeWrite(output)

    kryo.writeClassAndObject(output, beforeWrite)
    kryo.writeClassAndObject(output, afterRead)
  }

  override def read(kryo: Kryo, input: Input): Unit = {

    this.beforeWrite = kryo.readClassAndObject(input).asInstanceOf[OutputStream => Any]
    this.afterRead = kryo.readClassAndObject(input).asInstanceOf[InputStream => Any]

    afterRead(input)
  }
}
