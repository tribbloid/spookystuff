package com.tribbloids.spookystuff.utils

import java.nio.ByteBuffer

import com.tribbloids.spookystuff.utils.refl.{ScalaType, TypeUtils}
import org.apache.hadoop.io.Writable
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.spark.serializer.{JavaSerializer, KryoSerializer, SerializerInstance}
import org.apache.spark.sql.catalyst.ScalaReflection.universe.TypeTag
import org.apache.spark.{SerializableWritable, SparkConf}

import scala.reflect.ClassTag
import scala.util.Try

object BinaryWritable {

  val conf = new SparkConf()
    .registerKryoClasses(Array(classOf[TypeTag[_]]))

  val javaSerFactory: () => SerializerInstance = {
    () =>
      new JavaSerializer(conf).newInstance()
  }
  val kryoSerFactory: () => SerializerInstance = {
    () =>
      new KryoSerializer(conf).newInstance()
  }
}

class BinarySerializable[T: ClassTag](
                                       @transient val obj: T,
                                       val serFactory: () => SerializerInstance = BinaryWritable.javaSerFactory
                                     ) extends Serializable {

  @transient lazy val ser = serFactory()

  val binary: Array[Byte] = {
    ser.serialize(obj).array()
  }

  @transient lazy val value: T = Option(obj).getOrElse {
    ser.deserialize[T](ByteBuffer.wrap(binary))
  }
}

class BinaryWritable[T <: Writable](
                                     @transient val obj: T,
                                     val serFactory: () => SerializerInstance = BinaryWritable.javaSerFactory
                                   ) extends Serializable {

  val delegate: BinarySerializable[SerializableWritable[T]] = new BinarySerializable(
    new SerializableWritable(obj),
    serFactory
  )

  @transient lazy val value: T = Option(obj).getOrElse {
    delegate.value.value
  }
}

class SerializableUGI(
                       @transient val _ugi: UserGroupInformation,
                       val serFactory: () => SerializerInstance = BinaryWritable.javaSerFactory
                     ) extends Serializable {

  val name =  _ugi.getUserName
  val credentials: BinaryWritable[Credentials] = new BinaryWritable(_ugi.getCredentials, serFactory)

  @transient lazy val value: UserGroupInformation = Option(_ugi).getOrElse {
    val result = UserGroupInformation.createRemoteUser(name)
    result.addCredentials(credentials.value)
    result
  }
}

/**
  * this is a makeshift wrapper to circumvent scala 2.10 reflection's serialization problem
  */
class SerializableTypeTag[T](
                              @transient val _ttg: TypeTag[T]
                            ) extends Serializable {

  val delegateOpt = Try {
    new BinarySerializable(_ttg)
  }
    .toOption

  val dTypeOpt = TypeUtils.tryCatalystTypeFor(_ttg).toOption

  @transient lazy val value: TypeTag[T] = delegateOpt.map {
    v =>
      v.value
  }
    .orElse {
      dTypeOpt.flatMap {
        dType =>
          ScalaType.DTypeView(dType).scalaTypeOpt.map(_.asInstanceOf[TypeTag[T]])
      }
    }
    .getOrElse(
      throw new UnsupportedOperationException(
        "TypeTag is lost during serialization and unrecoverable from Catalyst DataType"
      )
    )
}
