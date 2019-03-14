package org.apache.spark.ml.dsl.utils.metadata

import com.tribbloids.spookystuff.utils.IDMixin

import scala.collection.immutable.ListMap

trait MetadataLike extends Serializable with IDMixin {
  def self: ListMap[String, Any]

  override def _id: Any = self

  //  def +(tuple: (Param[_], Any)) = this.copy(this.map + (tuple._1.key -> tuple._2))
  def ++(v2: MetadataLike) = new Metadata(this.self ++ v2.self)

  //TODO: support mixing param and map definition? While still being serializable?
  abstract class Param[T] {

    final val name: String = this.getClass.getSimpleName

    def alternativeNames: List[String] = Nil

    lazy val get: Option[T] = self.get(name).map(_.asInstanceOf[T])
    def apply(): T = get.get
  }

  abstract class ParamWithDefault[T](val default: T) extends Param[T] {

    require(default != null, s"default value of parameter `$name` cannot be null")

    def getOrDefault: T = get.getOrElse(default)
  }

  abstract class StringParam(override val default: String = "") extends ParamWithDefault[String](default) {
    //TODO: add shortcut to cast into boolean etc
  }

  //WARNING: DO NOT add implicit conversion to inner class' companion object! will trigger "java.lang.AssertionError: assertion failed: mkAttributedQualifier(_xxx ..." compiler error!
  //TODO: report bug to scala team!
  //  object Param {
  //    implicit def toStr(v: Param[_]): String = v.k
  //
  //    implicit def toKV[T](v: (Param[_], T)): (String, T) = v._1.k -> v._2
  //  }
}
