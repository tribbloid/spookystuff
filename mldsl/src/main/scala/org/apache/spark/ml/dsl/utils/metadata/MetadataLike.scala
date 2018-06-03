package org.apache.spark.ml.dsl.utils.metadata

import scala.collection.immutable.ListMap


trait MetadataLike {
  def map: ListMap[String, Any]

  //  def +(tuple: (Param[_], Any)) = this.copy(this.map + (tuple._1.key -> tuple._2))
  def ++(v2: MetadataLike) = Metadata(this.map ++ v2.map)

  //TODO: support mixing param and map definition? While still being serializable?
  trait Param[T] extends ParamLike {

    lazy val get: Option[T] = map.get(name).map(_.asInstanceOf[T])
    def apply(): T = get.get
  }

  //WARNING: DO NOT add implicit conversion to inner class' companion object! will trigger "java.lang.AssertionError: assertion failed: mkAttributedQualifier(_xxx ..." compiler error!
  //TODO: report bug to scala team!
  //  object Param {
  //    implicit def toStr(v: Param[_]): String = v.k
  //
  //    implicit def toKV[T](v: (Param[_], T)): (String, T) = v._1.k -> v._2
  //  }
}
