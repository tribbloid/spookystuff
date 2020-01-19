package com.tribbloids.spookystuff.utils

class BypassingRule {

  def apply(e: Throwable): Bypassing = {
    e match {
      case ee: Bypassing => ee
      case _             => new Bypassing(e)
    }
  }

  class Bypassing(cause: Throwable) extends Throwable("Bypassing: " + this.getClass.getSimpleName, cause)

  def during[T](f: => T): T = {
    try {
      f
    } catch {
      case e: Throwable => throw apply(e)
    }
  }
}

object BypassingRule {

  case object NoRetry extends BypassingRule
  case object Silent extends BypassingRule
}
