package org.apache.spark.ml.dsl.utils.messaging


trait Relay_>>[T] extends MessageRelay[T] {

  override type M = Any
  override def messageMF: Manifest[Any] = implicitly[Manifest[Any]]
}

trait HasRootTag {

  def rootTag: String = Codec.getDefaultRootTag(this)
}

trait ProtoAPI extends HasRootTag {

  def toMessage_>> : Any
}

object ProtoAPI extends Relay_>>[ProtoAPI]{

  override def toMessage_>>(v: ProtoAPI): Any = v.toMessage_>>
}

trait MessageAPI extends HasRootTag with Serializable {
}

object MessageAPI extends MessageReader[MessageAPI] {
}

trait MessageAPI_<< extends MessageAPI {

  def toProto_<< : Any
}
