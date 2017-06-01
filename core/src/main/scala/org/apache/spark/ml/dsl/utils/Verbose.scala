package org.apache.spark.ml.dsl.utils

/**
  * Created by peng on 31/05/17.
  */
trait Verbose {

  def toStringOverride = super.toString
  def detail: String = ""

  def showDetail(nonVerbose: String) = {
    val result = nonVerbose + Option(detail)
      .filter(_.nonEmpty).map("\n" + _).getOrElse("")
    result
  }

  def toStringDetailed = {
    showDetail(toStringOverride)
  }
}
