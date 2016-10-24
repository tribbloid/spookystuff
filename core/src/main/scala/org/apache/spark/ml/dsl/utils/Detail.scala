package org.apache.spark.ml.dsl.utils

/**
  * Created by peng on 24/10/16.
  */
trait Detail {

  def detail: String = ""

  def verbose(nonVerbose: String) = {
    val result = nonVerbose + Option(detail).filter(_.nonEmpty).map("\n" + _).getOrElse("")
    result
  }

  def toStringVerbose = {
    verbose(super.toString)
  }
}
