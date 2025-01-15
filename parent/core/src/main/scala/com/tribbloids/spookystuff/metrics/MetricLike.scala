package com.tribbloids.spookystuff.metrics

import org.apache.spark.ml.dsl.utils.ClassOpsMixin

trait MetricLike extends Product with ClassOpsMixin with Serializable {

  def displayNameOvrd: Option[String] = None

  lazy val displayName: String = displayNameOvrd.getOrElse(this.getClass.simpleName_Scala)
}
