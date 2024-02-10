package com.tribbloids.spookystuff.metrics

import ai.acyclic.prover.commons.same.EqualBy
import org.apache.spark.ml.dsl.utils.ClassOpsMixin

trait MetricLike extends Product with ClassOpsMixin with Serializable with EqualBy {

  def displayNameOvrd: Option[String] = None

  lazy val displayName: String = displayNameOvrd.getOrElse(this.getClass.simpleName_Scala)
}
