package com.tribbloids.spookystuff.utils

import scala.reflect.ClassTag

/**
  * Created by peng on 14/02/17.
  */
trait Static[T] {

  implicit def self: this.type = this
}
