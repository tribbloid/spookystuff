package com.tribbloids.spookystuff.utils

import scala.runtime.ScalaRunTime

/**
  * Created by peng on 01/04/16.
  */
trait PrettyToStringMixin extends Product {

  abstract override def toString = ScalaRunTime._toString(this)
}
