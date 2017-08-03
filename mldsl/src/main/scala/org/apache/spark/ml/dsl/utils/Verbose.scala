package org.apache.spark.ml.dsl.utils

import scala.runtime.ScalaRunTime

/**
  * Created by peng on 31/05/17.
  */
trait Verbose extends Product {

  def toStringOverride = ScalaRunTime._toString(this)
  def detail: String = ""

  def withDetail(str: String) = {
    val result = str + Option(detail)
      .filter(_.nonEmpty).map("\n" + _).getOrElse("")
    result
  }

  def toStringDetailed = {
    withDetail(toStringOverride)
  }
}
