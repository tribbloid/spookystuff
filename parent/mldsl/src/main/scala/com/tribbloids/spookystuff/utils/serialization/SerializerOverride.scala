package com.tribbloids.spookystuff.utils.serialization

import ai.acyclic.prover.commons.EqualBy
import org.apache.hadoop.io.Writable
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.{SerializableWritable, SparkConf}

import java.io
import java.nio.ByteBuffer
import scala.language.implicitConversions
import scala.reflect.ClassTag

object SerializerOverride {

  object Default extends OfSparkConf(new SparkConf())

  implicit def box[T: ClassTag](v: T): SerializerOverride[T] = SerializerOverride(v)

  implicit def unbox[T: ClassTag](v: SerializerOverride[T]): T = v.value
}

/**
  * automatically wrap with SerializableWritable when being serialized discard original value wrapping & unwrapping is
  * lazy
  */
case class SerializerOverride[T: ClassTag](
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
      // TODO: impl cannot handle mutable object, can this be delegated to BeforeAndAfter?
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
