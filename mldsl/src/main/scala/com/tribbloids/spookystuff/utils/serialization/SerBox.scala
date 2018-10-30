package com.tribbloids.spookystuff.utils.serialization

import java.nio.ByteBuffer

import com.tribbloids.spookystuff.utils.IDMixin
import org.apache.hadoop.io.Writable
import org.apache.spark.ml.dsl.utils.refl.{ScalaType, TypeUtils}
import org.apache.spark.serializer.{JavaSerializer, KryoSerializer, SerializerInstance}
import org.apache.spark.sql.catalyst.ScalaReflection.universe.TypeTag
import org.apache.spark.{SerializableWritable, SparkConf}

import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.util.Try

object SerBox {

  val conf = new SparkConf()
    .registerKryoClasses(Array(classOf[TypeTag[_]]))

  val javaSerializer = new JavaSerializer(conf)
  val javaOverride: () => SerializerInstance = { //TODO: use singleton?
    () =>
      javaSerializer.newInstance()
  }

  val kryoSerializer = new KryoSerializer(conf)
  val kryoOverride: () => SerializerInstance = { //TODO: use singleton?
    () =>
      kryoSerializer.newInstance()
  }

  val serializers = List(javaSerializer, kryoSerializer)

  implicit def boxing[T: ClassTag](v: T): SerBox[T] = SerBox(v)
}

/**
  * automatically wrap with SerializableWritable when being serialized
  * discard original value
  * wrapping & unwrapping is lazy
  * @param obj
  * @param serOverride
  * @tparam T
  */
case class SerBox[T: ClassTag](
    @transient obj: T,
    serOverride: () => SerializerInstance = null
) extends Serializable
    with IDMixin {

  @transient lazy val serOpt: Option[SerializerInstance] = Option(serOverride).map(_.apply)

  @transient lazy val serObj: Serializable = obj match {
    case ss: Serializable =>
      ss: Serializable
    case ww: Writable =>
      new SerializableWritable(ww) with Serializable
    case _ =>
      throw new UnsupportedOperationException(s"$obj is not Serializable or Writable")
  }

  val delegate: Either[Serializable, Array[Byte]] = {

    serOpt match {
      case None =>
        Left(serObj)
      case Some(serde) =>
        Right(serde.serialize(obj).array())
    }
  }

  @transient lazy val value: T = Option(obj).getOrElse {
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

/**
  * this is a makeshift wrapper to circumvent scala 2.10 reflection's serialization problem
  */
class WritableTypeTag[T](
    @transient val _ttg: TypeTag[T]
) extends Serializable {

  val boxOpt = Try {
    new SerBox(_ttg, SerBox.javaOverride) //TypeTag is incompatible with Kryo
  }.toOption

  val dTypeOpt = TypeUtils.tryCatalystTypeFor(_ttg).toOption

  @transient lazy val value: TypeTag[T] = Option(_ttg)
    .orElse {
      boxOpt.map { v =>
        v.value
      }
    }
    .orElse {
      dTypeOpt.flatMap { dType =>
        ScalaType.DataTypeView(dType).scalaTypeOpt.map(_.asInstanceOf[TypeTag[T]])
      }
    }
    .getOrElse(
      throw new UnsupportedOperationException(
        "TypeTag is lost during serialization and unrecoverable from Catalyst DataType"
      )
    )
}
