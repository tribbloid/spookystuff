package org.apache.spark.ml.dsl.utils.messaging

import org.apache.spark.ml.dsl.utils.refl.ReflectionUtils

import scala.collection.immutable.ListMap

object AutomaticRelay {}

// use reflection to find most qualified relay for the type of each field from their respective companion objects
abstract class AutomaticRelay[T <: Product: Manifest] extends Relay[T] {
  // TODO:
  //  slow in runtime, and unreliable
  //  in the next version, should be rewritten using shapeless Generic and prover through implicits

  override type Msg = Any

  override def toMessage_>>(v: T): Msg = {
    val prefix = v.productPrefix
    val kvs = Map(ReflectionUtils.getCaseAccessorMap(v): _*)

    val relayedKVs = kvs.mapValues { v =>
      val ir: TreeIR.Leaf[Any] = TreeIR.Leaf(v)
      ir.depthFirstTransform.onLeaf { v: Any =>
        v match {
          case vs: Seq[_] =>
            vs.map { v: Any =>
              val rr = RelayRegistry.Default.lookupOrDefault(v)
              rr.toMessage_>>(v)
            }
          case m: Map[_, _] =>
            val list = m.toList.map {
              case (k: Any, v: Any) =>
                val rr = RelayRegistry.Default.lookupOrDefault(v)
                k -> rr.toMessage_>>(v)
            }
            ListMap(list: _*)
          case v: Any =>
            val rr = RelayRegistry.Default.lookupOrDefault(v)
            rr.toMessage_>>(v)
        }

      }.execute
    }

    val relayed = TreeIR.Struct.Builder(Some(prefix)).fromKVs(relayedKVs.toSeq: _*)

    val result = relayed.toMessage_>>
    result
  }

  final override def toProto_<<(v: Msg, rootTag: String) = ??? // TODO: will be supported in the next version
}
