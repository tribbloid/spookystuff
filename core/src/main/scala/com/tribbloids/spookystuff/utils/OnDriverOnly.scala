package com.tribbloids.spookystuff.utils

/**
  * Created by peng on 15/09/16.
  */
class OnDriverMark

trait OnDriverOnly {

  @transient val mark = new OnDriverMark

  def isOnDriver = mark == null

  def exe[T](f: =>T) = {
    if (isOnDriver) throw new UnsupportedOperationException("Can only be executed on driver")
    else f
  }
}
