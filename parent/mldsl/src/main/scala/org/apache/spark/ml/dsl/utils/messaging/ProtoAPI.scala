package org.apache.spark.ml.dsl.utils.messaging

trait Relay_>>[T] extends MessageRelay[T] {

  override type M = Any
  override def messageMF: Manifest[Any] = implicitly[Manifest[Any]]
}

trait ProtoAPI extends RootTagged {

  def toMessage_>> : Any
}

object ProtoAPI extends Relay_>>[ProtoAPI] {

  override def toMessage_>>(v: ProtoAPI): Any = v.toMessage_>>
}

trait MessageAPI extends RootTagged with Serializable {}

object MessageAPI extends MessageReader[MessageAPI] {}

trait MessageAPI_<< extends MessageAPI {

  def toProto_<< : Any
}
