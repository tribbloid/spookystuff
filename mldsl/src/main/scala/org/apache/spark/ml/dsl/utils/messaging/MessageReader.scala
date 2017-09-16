package org.apache.spark.ml.dsl.utils.messaging

import scala.language.implicitConversions

/**
  * a simple MessageRelay that use object directly as Message
  */
sealed trait Level1 {

  implicit def fromMF[T](implicit mf: Manifest[T]) = new MessageReader[T]()(mf)
}

class MessageReader[Obj](
                          implicit override val mf: Manifest[Obj]
                        ) extends RelayLike[Obj] {
  type M = Obj

  override def toM(v: Obj) = v

  def reader = this
}

object MessageReader extends MessageReader[Any] with Level1 {
}