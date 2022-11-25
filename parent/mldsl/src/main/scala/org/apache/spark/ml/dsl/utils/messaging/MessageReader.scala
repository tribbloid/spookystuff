package org.apache.spark.ml.dsl.utils.messaging

import org.apache.spark.ml.dsl.utils.refl.ScalaType
import org.json4s.JValue

/**
  * a simple MessageRelay that use object directly as Message
  */
class MessageReader[Self](
    implicit
    override val messageMF: Manifest[Self] // TODO: change to ScalaType
) extends Codec[Self] {
  type M = Self

  override def selfType: ScalaType[Self] = messageMF

  override def toMessage_>>(v: Self): Self = v
  override def toProto_<<(v: Self, rootTag: String): Self = v
}

object MessageReader extends MessageReader[Any] with MessageReaderLevel1 {}

object JValueMessageReader extends MessageReader[JValue] with MessageReaderLevel1
