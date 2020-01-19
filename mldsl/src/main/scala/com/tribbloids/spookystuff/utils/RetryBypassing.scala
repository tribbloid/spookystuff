package com.tribbloids.spookystuff.utils

class RetryBypassing {

  def apply(e: Throwable): BypassedException = {
    e match {
      case ee: BypassedException => ee
      case _                     => new BypassedException(e)
    }
  }

  class BypassedException(cause: Throwable) extends RuntimeException("Bypassing: " + this.getClass.getSimpleName, cause)

  def during[T](f: => T): T = {
    try {
      f
    } catch {
      case e: Throwable => throw apply(e)
    }
  }
}

case object NoRetry extends RetryBypassing
case object Silent extends RetryBypassing
