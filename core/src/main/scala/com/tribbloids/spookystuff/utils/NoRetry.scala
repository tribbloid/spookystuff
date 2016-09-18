package com.tribbloids.spookystuff.utils

/**
  * Created by peng on 18/09/16.
  */
case object NoRetry {


  case class BypassRetryException(e: Throwable) extends RuntimeException("BYPASSING", e)

  def apply[T](f: =>T): T ={
    try {
      f
    }
    catch {
      case e: Throwable => throw BypassRetryException(e)
    }
  }
}
