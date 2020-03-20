package com.tribbloids.spookystuff.metrics

trait MetricLike extends Product with Serializable {

  def displayNameOvrd: Option[String] = None

  lazy val displayName: String = displayNameOvrd.getOrElse(this.productPrefix)
}
