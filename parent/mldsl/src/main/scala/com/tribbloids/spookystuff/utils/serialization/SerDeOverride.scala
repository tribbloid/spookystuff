package com.tribbloids.spookystuff.utils.serialization

import ai.acyclic.prover.commons.EqualBy
import org.apache.hadoop.io.Writable
import org.apache.spark.serializer.{JavaSerializer, KryoSerializer, Serializer, SerializerInstance}
import org.apache.spark.sql.catalyst.ScalaReflection.universe.TypeTag
import org.apache.spark.{SerializableWritable, SparkConf}

import java.io
import java.nio.ByteBuffer
import scala.language.implicitConversions
import scala.reflect.ClassTag

object SerDeOverride {

  case class WithConf(conf: SparkConf) {

    @transient lazy val _conf: SparkConf = conf
      .registerKryoClasses(Array(classOf[TypeTag[_]]))

    @transient lazy val javaSerializer: JavaSerializer = new JavaSerializer(_conf)
    @transient lazy val javaOverride: () => Some[SerializerInstance] = { // TODO: use singleton?
      () =>
        Some(javaSerializer.newInstance())
    }

    @transient lazy val kryoSerializer: KryoSerializer = new KryoSerializer(_conf)
    @transient lazy val kryoOverride: () => Some[SerializerInstance] = { // TODO: use singleton?
      () =>
        Some(kryoSerializer.newInstance())
    }

    @transient lazy val allSerializers: List[Serializer] = List(javaSerializer, kryoSerializer)
  }

  object Default extends WithConf(new SparkConf())

  implicit def box[T: ClassTag](v: T): SerDeOverride[T] = SerDeOverride(v)

  implicit def unbox[T: ClassTag](v: SerDeOverride[T]): T = v.value
}

/**
  * automatically wrap with SerializableWritable when being serialized discard original value wrapping & unwrapping is
  * lazy
  */
case class SerDeOverride[T: ClassTag](
    // TODO: replace with twitter MeatLocker?
    @transient private val _original: T,
    overrideImpl: () => Option[SerializerInstance] = () => None // no override by default
) extends Serializable
    with EqualBy {

  @transient lazy val serOpt: Option[SerializerInstance] = overrideImpl.apply

  @transient lazy val serObj: io.Serializable = _original match {
    case ss: Serializable =>
      ss
    case ss: java.io.Serializable =>
      ss
    case ww: Writable =>
      new SerializableWritable(ww)
    case _ =>
      throw new UnsupportedOperationException(
        s"${_original}: ${_original.getClass.getCanonicalName} is not Serializable or Writable"
      )
  }

  val delegate: Either[io.Serializable, Array[Byte]] = {

    serOpt match {
      case None =>
        Left(serObj)
      case Some(serde) =>
        Right(serde.serialize(serObj).array())
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

  override def samenessDelegatedTo: Any = value
}
