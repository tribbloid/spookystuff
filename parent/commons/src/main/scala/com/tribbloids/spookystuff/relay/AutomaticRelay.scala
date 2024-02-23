package com.tribbloids.spookystuff.relay

import com.tribbloids.spookystuff.commons.refl.ReflectionUtils

object AutomaticRelay {}

// use reflection to find most qualified relay for the type of each field from their respective companion objects
abstract class AutomaticRelay[T <: Product: Manifest] extends Relay[T] {
  // TODO:
  //  slow in runtime, and unreliable
  //  in the next version, should be rewritten using shapeless Generic and prover through implicits

  override type IR_>> = TreeIR.MapTree[String, Any]

  override def toMessage_>>(v: T): IR_>> = {
    val prefix = v.productPrefix
    val kvs = Map(ReflectionUtils.getCaseAccessorMap(v): _*)

    val relayedKVs: Map[String, TreeIR[Any]] = kvs.map {
      case (k, v) =>
        val ir: TreeIR.Leaf[Any] = TreeIR.leaf(v)
        val newV = ir.DepthFirstTransform
          .onLeaves[Any] { v =>
            v.body match {
              case vs: Seq[_] =>
                val list = vs.toList
                  .map { v: Any =>
                    val rr = RelayRegistry.Default.lookupOrDefault(v)
                    rr.toMessage_>>(v).asInstanceOf[TreeIR[Any]]
                    // TODO: Cast may fail, IR should be an extendable interface
                  }
                TreeIR.list(list: _*)
              case m: Map[_, _] =>
                val list = m.toList.map {
                  case (k: Any, v: Any) =>
                    val rr = RelayRegistry.Default.lookupOrDefault(v)
                    k -> rr.toMessage_>>(v).asInstanceOf[TreeIR[_]]
                }
                TreeIR.map(list: _*)
              case v: Any =>
                val rr = RelayRegistry.Default.lookupOrDefault(v)
                rr.toMessage_>>(v).asInstanceOf[TreeIR[Any]]
            }

          }
          .execute

        k -> newV
    }

    val relayed = TreeIR.Builder(Some(prefix)).map(relayedKVs.toSeq: _*).schematic

    relayed
  }

//  final override def toProto_<<(v: Aux[ListMap[String, Any]]): T = ???
  override def toProto_<<(v: IR_<<): T = ???
}
