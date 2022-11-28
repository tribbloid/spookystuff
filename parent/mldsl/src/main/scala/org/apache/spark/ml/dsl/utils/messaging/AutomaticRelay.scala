package org.apache.spark.ml.dsl.utils.messaging

import org.apache.spark.ml.dsl.utils.refl.ReflectionUtils

object AutomaticRelay {}

// use reflection to find most qualified relay for the type of each field from their respective companion objects
abstract class AutomaticRelay[T <: Product: Manifest] extends MessageRelay[T] {
  // TODO:
  //  slow in runtime, and unreliable
  //  in the next version, should be rewritten using shapeless Generic and prover through implicits

  override type M = Any
  override def messageMF = implicitly[Manifest[M]]

  override def toMessage_>>(v: T): M = {
    val prefix = v.productPrefix
    val kvs = Map(ReflectionUtils.getCaseAccessorMap(v): _*)

    val relayedKVs = kvs.mapValues { v =>
      TreeIR
        .Value(v)
        .depthFirstTransform(
          onValue = { v: Any =>
            val codec = CodecRegistry.Default.findCodecOrDefault(v)
            codec.toMessage_>>(v)
          }
        )
        .self
    }

    val relayed = TreeIR.fromKVs(relayedKVs.toSeq: _*)

    val result = relayed.toMessage_>>
    result
  }
}
