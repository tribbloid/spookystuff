package org.apache.spark.ml.dsl.utils.messaging

trait SelfAPI {

  def toMessage_>> : Any
}

trait SelfRelay[T] extends MessageRelay[T] {

  override type M = Any
  override def messageMF: Manifest[Any] = implicitly[Manifest[Any]]
}

object SelfAPI extends SelfRelay[SelfAPI]{

  override def toMessage_>>(v: SelfAPI): Any = v.toMessage_>>
}

//trait RecursiveSelfAPI {
//}

//object RecursiveSelfAPI extends MessageRelay[RecursiveSelfAPI]{
//
//  override type M = Any
//  override def messageMF: Manifest[Any] = implicitly[Manifest[Any]]
//
//  override def toMessage_>>(v: RecursiveSelfAPI): Any = {
//    val transformed: Nested[Any] = Nested[Any](v).map[Any] {
//      v: Any =>
//        val codec = Registry.Default.findCodecOrDefault(v)
//        assert(codec != RecursiveSelfAPI) //avoid endless recursion
//        codec.toMessage_>>(v)
//    }
//
//    transformed.self
//  }
//}

trait MessageAPI {
}

object MessageAPI extends MessageReader[MessageAPI] {
}

trait MessageAPI_<=>[Self] extends MessageAPI {

  def toSelf_<< : Self
}
