package org.apache.spark.ml.dsl.utils.data

import org.apache.spark.ml.dsl.utils.data.EAV.Impl

import scala.collection.immutable.ListMap
import scala.reflect.ClassTag

case class EAVCore(
    self: ListMap[String, Any] = ListMap.empty
) extends EAV.ImplicitSrc {

  override type VV = Any
  override def ctg: ClassTag[VV] = getCtg

  final override lazy val source: Impl = this

  override def defaultSeparator = ", "
  override def toString: String =
    s"<${_ctg.runtimeClass.getSimpleName}>: {${formattedStr()}}"
}

object EAVCore extends EAVRelay[EAVCore] {

  override def fromCore(v: Impl): Impl = v
}
