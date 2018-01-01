package org.apache.spark.ml.dsl.utils.messaging


trait Relay_>>[T] extends MessageRelay[T] {

  override type M = Any
  override def messageMF: Manifest[Any] = implicitly[Manifest[Any]]
}

trait ProtoAPI {

  def toMessage_>> : Any
}

object ProtoAPI extends Relay_>>[ProtoAPI]{

  override def toMessage_>>(v: ProtoAPI): Any = v.toMessage_>>
}

trait MessageAPI {
}

object MessageAPI extends MessageReader[MessageAPI] {
}

trait MessageAPI_<=>[Self] extends MessageAPI {

  def toSelf_<< : Self
}
