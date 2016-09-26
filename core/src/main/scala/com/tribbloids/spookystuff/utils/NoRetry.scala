package com.tribbloids.spookystuff.utils

import com.tribbloids.spookystuff.utils.NoRetry.NoRetryWrapper

import scala.util.{Failure, Success, Try}

/**
  * Created by peng on 18/09/16.
  */
case object NoRetry {

  object NoRetryWrapper {

    def apply(e: Throwable) = {
      e match {
        case ee: NoRetryWrapper => ee
        case _ => new NoRetryWrapper(e)
      }
    }
  }

  class NoRetryWrapper(cause: Throwable) extends RuntimeException("BYPASSING", cause)

  def apply[T](f: =>T): T ={
    try {
      f
    }
    catch {
      case e: Throwable => throw NoRetryWrapper(e)
    }
  }
}

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
      case Failure(ee: NoRetryWrapper) =>
        NoRetry {
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

  def get: T = SpookyUtils.retry(n, interval = this.interval)(f)
}
