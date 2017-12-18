package org.apache.spark.ml.dsl.utils.messaging

import org.apache.spark.ml.dsl.utils.refl.ReflectionUtils

import scala.collection.immutable.ListMap

//TODO: add type information
case class GenericProduct[T <: Product: Manifest](
                                                   override val productPrefix: String,
                                                   kvs: ListMap[String, Any]
                                                 ) extends Product with Map[String, Any] {

  override def productElement(n: Int): Any = kvs.toSeq(n)._2

  override def productArity: Int = kvs.size

  override def +[B1 >: Any](kv: (String, B1)): Map[String, B1] = this.copy(kvs = kvs + kv)

  override def get(key: String): Option[Any] = kvs.get(key)

  override def iterator: Iterator[(String, Any)] = kvs.iterator

  override def -(key: String): Map[String, Any] = this.copy(kvs = kvs - key)
}

abstract class AutomaticRelay[T <: Product: Manifest] extends MessageRelay[T] {

  override type M = GenericProduct[T]
  override def messageMF = implicitly[Manifest[M]]

  override def toMessage_>>(v: T): GenericProduct[T] = {
    val prefix = v.productPrefix
    val kvs = Map(ReflectionUtils.getCaseAccessorMap(v): _*)

    val transformedKVs = kvs.mapValues {
      v =>
        Nested[Any](v).map[Any] {
          v: Any =>
            val codec = Registry.Default.findCodecOrDefault(v)
            codec.toMessage_>>(v)
        }
          .self
    }

    val casted = ListMap(transformedKVs.toSeq: _*)

    val result = GenericProduct[T](prefix, casted)
    result
  }
}
