package com.tribbloids.spookystuff.utils

import scala.runtime.ScalaRunTime

/**
  * Created by peng on 01/04/16.
  */
trait PrettyProduct extends Product {

  abstract override def toString = ScalaRunTime._toString(this)

  def toStringVerbose = ScalaRunTime._toString(this) + Option(extraToString).map("\n" + _).getOrElse("")

  def extraToString: String = null
}
