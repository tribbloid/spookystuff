package com.tribbloids.spookystuff.utils

trait OnDriverOnly {

  @transient val marker = Nil

  def notOnDriver = marker == null
  def isOnDriver = !notOnDriver

  def exe[T](f: =>T) = {
    if (notOnDriver) throw new UnsupportedOperationException("Can only be executed on driver")
    else f
  }
}
