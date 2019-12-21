package com.tribbloids.spookystuff.utils.serialization

import java.io
import java.nio.ByteBuffer

import com.tribbloids.spookystuff.utils.IDMixin
import org.apache.hadoop.io.Writable
import org.apache.spark.serializer.{JavaSerializer, KryoSerializer, SerializerInstance}
import org.apache.spark.sql.catalyst.ScalaReflection.universe.TypeTag
import org.apache.spark.{SerializableWritable, SparkConf}

import scala.language.implicitConversions
import scala.reflect.ClassTag

object SerDeOverride {

  case class WithConf(conf: SparkConf = new SparkConf()) {

    val _conf: SparkConf = conf
      .registerKryoClasses(Array(classOf[TypeTag[_]]))

    val javaSerializer = new JavaSerializer(_conf)
    val javaOverride: () => Some[SerializerInstance] = { //TODO: use singleton?
      () =>
        Some(javaSerializer.newInstance())
    }

    val kryoSerializer = new KryoSerializer(_conf)
    val kryoOverride: () => Some[SerializerInstance] = { //TODO: use singleton?
      () =>
        Some(kryoSerializer.newInstance())
    }

    val allSerializers = List(javaSerializer, kryoSerializer)
  }

  object Default extends WithConf

  implicit def box[T: ClassTag](v: T): SerDeOverride[T] = SerDeOverride(v)

  implicit def unbox[T: ClassTag](v: SerDeOverride[T]): T = v.value
}

/**
  * automatically wrap with SerializableWritable when being serialized
  * discard original value
  * wrapping & unwrapping is lazy
  */
case class SerDeOverride[T: ClassTag](
    @transient private val _original: T,
    overrideImpl: () => Option[SerializerInstance] = () => None // no override by default
) extends Serializable
    with IDMixin {

  @transient lazy val serOpt: Option[SerializerInstance] = overrideImpl.apply

  @transient lazy val serObj: io.Serializable = _original match {
    case ss: Serializable =>
      ss: Serializable
    case ww: Writable =>
      new SerializableWritable(ww)
    case _ =>
      throw new UnsupportedOperationException(s"${_original} is not Serializable or Writable")
  }

  val delegate: Either[io.Serializable, Array[Byte]] = {

    serOpt match {
      case None =>
        Left(serObj)
      case Some(serde) =>
        Right(serde.serialize(_original).array())
    }
  }

  @transient lazy val value: T = Option(_original).getOrElse {
    (serOpt, delegate) match {
      case (None, Left(_serObj)) =>
        _serObj match {
          case v: SerializableWritable[_] => v.value.asInstanceOf[T]
          case _                          => _serObj.asInstanceOf[T]
        }
      case (Some(serde), Right(array)) =>
        serde.deserialize[T](ByteBuffer.wrap(array))
      case _ =>
        throw new UnknownError("IMPOSSIBLE!")
    }
  }

  override def _id: Any = value
}

//TODO: cleanup, useless, can be completely superceded by SerializableWritable?
//class BinaryWritable[T <: Writable](
//                                     @transient val obj: T,
//                                     val serFactory: () => SerializerInstance = BinaryWritable.javaSerFactory
//                                   ) extends Serializable {
//
//  val delegate: BinarySerializable[SerializableWritable[T]] = new BinarySerializable(
//    new SerializableWritable(obj),
//    serFactory
//  )
//
//  @transient lazy val value: T = Option(obj).getOrElse {
//    delegate.value.value
//  }
//}

//class SerializableUGI(
//                       @transient val _ugi: UserGroupInformation,
//                       val serFactory: () => SerializerInstance = BinaryWritable.javaSerFactory
//                     ) extends Serializable {
//
//  val name =  _ugi.getUserName
//  val credentials: BinaryWritable[Credentials] = new BinaryWritable(_ugi.getCredentials, serFactory)
//
//  @transient lazy val value: UserGroupInformation = Option(_ugi).getOrElse {
//    val result = UserGroupInformation.createRemoteUser(name)
//    result.addCredentials(credentials.value)
//    result
//  }
//}
