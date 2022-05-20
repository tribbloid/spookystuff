package com.tribbloids.spookystuff.utils.serialization

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import org.slf4j.LoggerFactory

import java.io.{InputStream, ObjectInputStream, ObjectOutputStream}

import scala.language.implicitConversions

trait BeforeAndAfterShipping extends Serializable {

  {
    _container
  }

  import BeforeAndAfterShipping._

  def beforeDeparture(): Unit = {}

  def afterArrival(): Unit = {}

  @transient private lazy val _container: Container[this.type] = Container(this)

  type ForShipping = Container[this.type]
  def forShipping: ForShipping = _container
}

object BeforeAndAfterShipping {

  @inline
  def logMsg(v: String): Unit = {
//    LoggerFactory.getLogger(this.getClass).debug(v: String)
  }

  object Container {

    implicit def unbox[T <: BeforeAndAfterShipping](v: Container[T]): T = v.value
  }

  case class Container[+T <: BeforeAndAfterShipping](
      @transient private var _value: BeforeAndAfterShipping
  ) extends Serializable
      with KryoSerializable {

    final def value: T = _value.asInstanceOf[T]

    def this() = this(null.asInstanceOf[T]) //TODO: useless?

    private def writeObject(aOutputStream: ObjectOutputStream): Unit = {
      _value.beforeDeparture()
      logMsg(s"JavaW: ${_value}")
      aOutputStream.writeObject(_value)
    }

    private def readObject(aInputStream: ObjectInputStream): Unit = {
      _value = aInputStream.readObject().asInstanceOf[T]
//      assert(isEOF(aInputStream), "not EOF!")

      logMsg(s"JavaR: ${_value}")

      _value.afterArrival()
    }

    def isEOF(v: InputStream): Boolean = {
      val vv = v.read()
      vv == -1
    }

    override def write(kryo: Kryo, output: Output): Unit = {
      _value.beforeDeparture()
      logMsg(s"KryoW: ${_value}")
      kryo.writeClassAndObject(output, _value)
    }

    override def read(kryo: Kryo, input: Input): Unit = {
      _value = kryo.readClassAndObject(input).asInstanceOf[BeforeAndAfterShipping]
//      assert(isEOF(input), "not EOF!")

      logMsg(s"KryoR: ${_value}")
      _value.afterArrival()
    }
  }
}
