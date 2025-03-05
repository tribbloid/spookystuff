package com.tribbloids.spookystuff.commons

import scala.runtime.ScalaRunTime

/**
  * Created by peng on 31/05/17.
  */
trait Verbose extends Product { // TODO: this class has to go

  def shortStr: String = ScalaRunTime._toString(this)
  def detail: String = ""

  def withDetail(str: String): String = {
    val result = str + Option(detail)
      .filter(_.nonEmpty)
      .map("\n" + _)
      .getOrElse("")
    result
  }

  def detailedStr: String = {
    withDetail(shortStr)
  }
}
