package org.apache.spark.ml.dsl.utils.messaging

import org.apache.spark.ml.dsl.utils.refl.{ReflectionUtils, RuntimeTypeOverride, ScalaType}

import scala.collection.immutable.ListMap

object AutomaticRelay {

  // TODO: add type information, and merge into RelayIR
  case class GenericProduct[T <: Product: Manifest](
      override val productPrefix: String,
      kvs: ListMap[String, Any],
      runtimeType: ScalaType[_]
  ) extends MessageAPI
      with Map[String, Any]
      with RuntimeTypeOverride {

    override def rootTag: String = productPrefix

    override def productElement(n: Int): Any = kvs.toSeq(n)._2

    override def productArity: Int = kvs.size

    override def +[B1 >: Any](kv: (String, B1)): Map[String, B1] = this.copy(kvs = kvs + kv)

    override def get(key: String): Option[Any] = kvs.get(key)

    override def iterator: Iterator[(String, Any)] = kvs.iterator

    override def -(key: String): Map[String, Any] = this.copy(kvs = kvs - key)
  }

}

// use reflection to find most qualified relay for the type of each field from their respective companion objects
// slow in runtime, and unreliable
// TODO: in the next version, should be rewritten using shapeless Generic and prover through implicits
abstract class AutomaticRelay[T <: Product: Manifest] extends MessageRelay[T] {

  import AutomaticRelay._

  override type M = GenericProduct[T]
  override def messageMF: Manifest[GenericProduct[T]] = implicitly[Manifest[M]]

  override def toMessage_>>(v: T): GenericProduct[T] = {
    val prefix = v.productPrefix
    val kvs = Map(ReflectionUtils.getCaseAccessorMap(v): _*)

    val transformedKVs = kvs.mapValues { v =>
      RelayIR
        .Value(v)
        .depthFirstTransform(
          onValue = { v: Any =>
            val codec = CodecRegistry.Default.findCodecOrDefault(v)
            codec.toMessage_>>(v)
          }
        )
        .self
    }

    val casted = ListMap(transformedKVs.toSeq: _*)

    val result = GenericProduct[T](prefix, casted, v.getClass)
    result
  }
}
