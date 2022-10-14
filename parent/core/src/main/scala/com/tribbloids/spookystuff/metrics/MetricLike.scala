package com.tribbloids.spookystuff.metrics

import com.tribbloids.spookystuff.utils.IDMixin
import org.apache.spark.ml.dsl.utils.ScalaNameMixin

trait MetricLike extends Product with ScalaNameMixin with Serializable with IDMixin {

  def displayNameOvrd: Option[String] = None

  lazy val displayName: String = displayNameOvrd.getOrElse(this.objectSimpleName)
}
