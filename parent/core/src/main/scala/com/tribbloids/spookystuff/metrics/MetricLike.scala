package com.tribbloids.spookystuff.metrics

import com.tribbloids.spookystuff.utils.EqualBy
import org.apache.spark.ml.dsl.utils.ObjectSimpleNameMixin

trait MetricLike extends Product with ObjectSimpleNameMixin with Serializable with EqualBy {

  def displayNameOvrd: Option[String] = None

  lazy val displayName: String = displayNameOvrd.getOrElse(this.objectSimpleName)
}
