package com.tribbloids.spookystuff.metrics

import com.tribbloids.spookystuff.commons.refl.ClassOpsMixin

trait MetricLike extends Product with ClassOpsMixin with Serializable {

  def displayNameOvrd: Option[String] = None

  lazy val displayName: String = displayNameOvrd.getOrElse(this.getClass.simpleName_Scala)
}
