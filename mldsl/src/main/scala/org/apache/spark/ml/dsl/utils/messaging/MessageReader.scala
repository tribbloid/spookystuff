package org.apache.spark.ml.dsl.utils.messaging

/**
  * a simple MessageRelay that use object directly as Message
  */
class MessageReader[Obj](
                          implicit override val mf: Manifest[Obj]
                        ) extends MessageRelayLike[Obj] {
  type M = Obj

  override def toM(v: Obj) = v

  def reader = this
}

object MessageReader extends MessageReader[Any] {
}