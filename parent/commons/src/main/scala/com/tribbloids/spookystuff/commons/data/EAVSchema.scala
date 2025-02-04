package com.tribbloids.spookystuff.commons.data

import ai.acyclic.prover.commons.multiverse.UnappliedForm
import ai.acyclic.prover.commons.util.Magnet.OptionMagnet
import com.tribbloids.spookystuff.relay.io.Encoder.HasEncoder
import com.tribbloids.spookystuff.relay.{Relay, TreeIR}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

import scala.collection.immutable.ListMap
import scala.language.implicitConversions

trait EAVSchema {

  protected trait EAVMixin extends EAVLike {

    final override def system: EAVSchema = EAVSchema.this

    @transient lazy val canonical: EAV = EAV(Map(KVs.defined*))
  }

  object EAVMixin extends HasEncoder[EAV] {

    // TODO: don't know how to do this, causing problem in generating KVs
    trait CaseInsensitive extends EAVMixin {

      override protected def getLookup: Map[String, Any] = CaseInsensitiveMap(super.getLookup)
    }

    implicit def relay: Relay[EAV] = _Relay()
  }

  /**
    * TODO: use [[ai.acyclic.prover.commons.function.Mk]] after Scala upgrade
    * @return
    *   constructor of ^
    */

  type EAV <: EAVMixin
  def EAV(v: collection.Map[String, Any]): EAV

  @transient lazy val empty: EAV = BuildFrom.seqInternal(Nil)

  class BuildFrom(
      private val constructor: collection.Map[String, Any] => EAV
  ) {

    type V = Any

    private[data] def seqInternal(kvs: Seq[(String, V)]): EAV = {

      constructor(ListMap[String, V](kvs*))
    }

//    def seq(kvs: Seq[Tuple2[String, V]]): EAV = {
//      val _kvs = kvs.map {
//        case (k: EAVMixin#Attr[_], v) =>
//          k.name -> v
//        case (k: String, v) =>
//          k -> v
//        case (k, _) =>
//          throw new UnsupportedOperationException(s"unsupported key type for $k")
//      }
//
//      seqUnsafe(_kvs)
//    }

//    def tuples(kvs: Tuple2[?, V]*): EAV = {
//      seq(kvs)
//    }

    def unappliedForm(opt: OptionMagnet[UnappliedForm]): EAV = {

      opt.original match {
        case Some(v) =>
          val actualKVs = v.kvPairs.collect {
            case (Some(k), v) =>
              k -> v
          }
          seqInternal(actualKVs)

        case None => seqInternal(Nil)
      }
    }

  }
  case object BuildFrom extends BuildFrom(v => EAV(v)) {}

  def apply(kvs: Magnets.AttrValueMag[?]*): EAV = {
    val _kvs: Seq[(String, Any)] = kvs.flatMap { m =>
      m.vOpt.map { v =>
        m.k -> v
      }
    }

    BuildFrom.seqInternal(_kvs)
  }

  case class _Relay() extends Relay[EAV] {

    final type IR_>> = TreeIR[Any]
    final type IR_<< = TreeIR.Leaf[Any]

    override def toMessage_>>(eav: EAV): IR_>> = {
      val raw: TreeIR.Leaf[EAV] = TreeIR.leaf(eav)

      val expanded = raw.DepthFirstTransform
        .down[Any] {
          case ll @ TreeIR.Leaf(v: EAVLike, _) =>
            val sub = v.KVs.raw
            val subNodes: Seq[(String, TreeIR.Leaf[Any])] = sub.map {
              case (kk, vv) =>
                kk -> TreeIR.leaf[Any](vv)
            }

            TreeIR.Builder(Some(ll.rootTag)).map(subNodes*)
          case others @ _ =>
            others
        }
        .execute

      expanded
    }

    override def toProto_<<(m: IR_<<): EAV = {

      val canonical = {
        m.explode.explodeStringMap()
      }

      val folded = canonical.DepthFirstTransform
        .up[Any, TreeIR.Leaf[Any]] {
          case struct: TreeIR.MapTree[_, _] =>
            val map = struct.body
            val stringMap = map.map {
              case (k, v) =>
                ("" + k) -> v
            }
            val eav = BuildFrom.seqInternal(stringMap.toSeq)
            TreeIR.Builder(Some(struct.rootTag)).leaf(eav)
          case ll: TreeIR.ListTree[_] =>
            TreeIR.Builder(Some(ll.rootTag)).leaf(ll.body)
          case ll: TreeIR.Leaf[_] =>
            ll.upcast[Any]
        }
        .execute

      folded.body.asInstanceOf[EAV]
    }
  }

  implicit def sys: EAVSchema.Aux[EAV] = EAVSchema.this

  def relay: _Relay = _Relay()

  implicit def toRelay(v: this.type): _Relay = relay
}

object EAVSchema {

  type Aux[T] = EAVSchema { type EAV = T }

  object NoAttr extends EAVSchema {

    implicit class EAV(val internal: collection.Map[String, Any]) extends EAVMixin {}

  }
  type NoAttr = NoAttr.EAV

}
