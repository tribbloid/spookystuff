package com.tribbloids.spookystuff.utils

import scala.util.{Failure, Success, Try}

/**
  * Created by peng on 18/09/16.
  */
class Bypassing {

  object Wrapper {

    def apply(e: Throwable) = {
      e match {
        case ee: Wrapper => ee
        case _ => new Wrapper(e)
      }
    }
  }

  class Wrapper(cause: Throwable) extends RuntimeException("Bypassing: " + this.getClass.getSimpleName, cause)

  def mapException[T](f: =>T): T ={
    try {
      f
    }
    catch {
      case e: Throwable => throw Wrapper(e)
    }
  }
}

case object NoRetry extends Bypassing
case object SilentRetry extends Bypassing

object Retry {

  def apply[T](n: Int, interval: Long = 0)(f: =>T) = new Retry(f, n, interval)
}

class Retry[T](
                f: =>T,
                n: Int,
                interval: Long = 0
              ) {

  def map[T2](g: Try[T] => T2): Retry[T2] = {

    val effectiveG: (Try[T]) => T2 = {
      case Failure(ee: NoRetry.Wrapper) =>
        NoRetry.mapException {
          g(Failure[T](ee.getCause))
        }
      case v =>
        g(v)
    }

    new Retry[T2](
      effectiveG(Try{f}),
      n,
      interval
    )
  }

  def mapSuccess[T2](g: T => T2): Retry[T2] = {
    val effectiveG: (Try[T]) => T2 = {
      case Success(v) => g(v)
      case Failure(ee) => throw ee
    }

    map(effectiveG)
  }

  def get: T = CommonUtils.retry(n, interval = this.interval)(f)
}
