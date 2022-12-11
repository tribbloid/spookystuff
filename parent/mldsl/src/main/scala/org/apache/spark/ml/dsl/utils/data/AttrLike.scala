package org.apache.spark.ml.dsl.utils.data

import com.tribbloids.spookystuff.utils.{EqualBy, TreeThrowable}
import org.apache.spark.ml.dsl.utils.?

import scala.util.Try

trait AttrLike[T] extends Serializable with EqualBy {

  def primaryName: String
  def aliases: List[String]

  final lazy val allNames: Seq[String] = (Seq(primaryName) ++ aliases).distinct

  def ->(v: T): Magnets.AttrValueMag[T] = {

    Magnets.AttrValueMag[T](this.primaryName, Some(v))
  }

  def -?>(vOpt: T `?` _): Magnets.AttrValueMag[T] = {

    Magnets.AttrValueMag[T](this.primaryName, vOpt.asOption)
  }

  def explicitValue: T
  def defaultValue: T

  final lazy val tryGet: Try[T] = {
    val trials: Seq[() => T] = Seq(
      () => explicitValue,
      () => defaultValue
    )

    Try {
      TreeThrowable
        .|||^(trials)
        .get
    }
  }

  final def get: Option[T] = tryGet.toOption
  final def value: T = tryGet.get

  override def _equalBy: Any = this.allNames -> get
}

object AttrLike {

//  implicit def toV[T](attr: AttrLike[T]): T = attr.value
}
