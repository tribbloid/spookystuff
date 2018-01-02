package org.apache.spark.ml.dsl.utils.messaging

import org.apache.spark.ml.dsl.utils.refl.ScalaType

import scala.language.implicitConversions

/**
  * a simple MessageRelay that use object directly as Message
  */
sealed trait MessageReaderLevel1 {

  implicit def fromMF[T](implicit mf: Manifest[T]) = new MessageReader[T]()(mf)
}

class MessageReader[Self](
                           implicit override val messageMF: Manifest[Self] //TODO: change to ScalaType
                         ) extends Codec[Self] {
  type M = Self

  override def selfType: ScalaType[Self] = messageMF

  override def toMessage_>>(v: Self) = v
  override def toProto_<<(v: Self): Self = v
}

object MessageReader extends MessageReader[Any] with MessageReaderLevel1 {
}
