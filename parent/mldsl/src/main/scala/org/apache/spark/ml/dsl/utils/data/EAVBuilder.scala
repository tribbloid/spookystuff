package org.apache.spark.ml.dsl.utils.data

import org.apache.spark.ml.dsl.utils.refl.ReflectionUtils

import scala.collection.immutable.ListMap
import scala.language.implicitConversions

trait EAVBuilder[I <: EAV] {

  type Impl <: I
  def Impl: EAVRelay[Impl]

  type VV = Impl#VV

  final def fromUntypedTuples(kvs: Tuple2[_, Any]*): Impl = {
    val _kvs = kvs.map {
      case (k: I#Attr[_], v) =>
        k.primaryName -> v
      case (k: String, v) =>
        k -> v
      case (k, v) =>
        throw new UnsupportedOperationException(s"unsupported key type for $k")
    }

    Impl.fromCore(EAV.Impl(ListMap(_kvs: _*)).core)
  }

  final def apply(kvs: Magnets.KV[_ <: VV]*): Impl = {
    val _kvs = kvs.flatMap { m =>
      m.vOpt.map { v =>
        m.k -> v
      }
    }

    fromUntypedTuples(_kvs: _*)
  }

  final def fromCaseClass(v: Product): Impl = {

    val kvMap: Seq[(String, Any)] = ReflectionUtils.getCaseAccessorMap(v)

    val kvMapFlattenOpt = kvMap.flatMap {
      case (k, vMaybeOpt) =>
        vMaybeOpt match {
          case Some(vv) => Some(k -> vv)
          case None     => None
          case _        => Some(k -> vMaybeOpt)
        }
    }

    fromUntypedTuples(kvMapFlattenOpt: _*)
  }

  lazy final val proto: Impl = apply()

  final def fromEAV(v: EAV): I = Impl.fromCore(v.core)

  final implicit def fromEAVImplicitly(v: EAV.ImplicitSrc): I = Impl.fromCore(v.core)

  final implicit def fromEAVOptImplicitly(vOpt: Option[EAV.ImplicitSrc]): Option[I] = vOpt.map(fromEAVImplicitly)

  final implicit def fromMap(map: Iterable[(_, VV)]): I = {

    fromUntypedTuples(map.toSeq: _*)
  }
}
