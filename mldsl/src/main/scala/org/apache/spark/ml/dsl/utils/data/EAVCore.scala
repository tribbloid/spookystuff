package org.apache.spark.ml.dsl.utils.data

import org.apache.spark.ml.dsl.utils.data.EAV.Impl

import scala.collection.immutable.ListMap
import scala.reflect.ClassTag

case class EAVCore(
    self: ListMap[String, Any] = ListMap.empty
) extends EAV.ImplicitSrc {

  override type VV = Any
  override val ctg: ClassTag[VV] = _ctg

  final override lazy val source: Impl = this
}

object EAVCore extends EAVRelay[EAVCore] {

  override def fromCore(v: Impl): Impl = v
}
