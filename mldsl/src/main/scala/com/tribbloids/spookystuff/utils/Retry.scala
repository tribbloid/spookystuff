package com.tribbloids.spookystuff.utils

import org.apache.spark.ml.dsl.utils.FlowUtils
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

/**
  * Created by peng on 18/09/16.
  */
object Retry {

  object FixedInterval {

    def apply(
        n: Int,
        interval: Long = 0L,
        silent: Boolean = false,
        callerStr: String = null
    ): Retry =
      Retry(n, { _ =>
        interval
      }, silent, callerStr)
  }

  object ExponentialBackoff {

    def apply(
        n: Int,
        longestInterval: Long = 0L,
        expBase: Double = 2.0,
        silent: Boolean = false,
        callerStr: String = null
    ): Retry =
      Retry(n, { n =>
        (longestInterval.doubleValue() / Math.pow(expBase, n - 2)).asInstanceOf[Long]
      }, silent, callerStr)
  }
}

case class Retry(
    n: Int = 3,
    intervalFactory: Int => Long = { _ =>
      0L
    },
    silent: Boolean = false,
    showStr: String = null
) {

  def apply[T](fn: => T): T = {

    new RetryImpl[T](() => fn).get(this)
  }

  def getImpl[T](fn: => T): RetryImpl[T] = {
    new RetryImpl[T](() => fn, this)
  }
}

object DefaultRetry extends Retry

case class RetryImpl[T](
    fn: () => T,
    defaultRetry: Retry = DefaultRetry
) {

  def get: T = get(defaultRetry)

  @annotation.tailrec
  final def get(
      retry: Retry
  ): T = {

    import retry._

    //TODO: merge with CommonUtils
    lazy val _callerShowStr = {
      Option(showStr).getOrElse {
        FlowUtils
          .Caller(
            exclude = Seq(classOf[Retry], classOf[RetryImpl[_]], classOf[CommonUtils])
          )
          .showStr
      }
    }

    lazy val interval = intervalFactory(n)
    Try { fn() } match {
      case Success(x) =>
        x
      case Failure(e: NoRetry.BypassedException) =>
        throw e.getCause
      case Failure(e) if n > 1 =>
        if (!(silent || e.isInstanceOf[Silent.BypassedException])) {
          val logger = LoggerFactory.getLogger(this.getClass)
          logger.warn(
            s"Retrying locally on ${e.getClass.getSimpleName} in ${interval.toDouble / 1000} second(s)... ${n - 1} time(s) left" +
              "\t@ " + _callerShowStr +
              "\n" + e.getMessage
          )
          logger.debug("\t\\-->", e)
        }
        Thread.sleep(interval)
        get(retry.copy(n = n - 1))
      case Failure(e) =>
        throw e
    }
  }

  def map[T2](g: Try[T] => T2): RetryImpl[T2] = {

    val effectiveG: Try[T] => T2 = {
      case Failure(ee: NoRetry.BypassedException) =>
        NoRetry.during {
          g(Failure[T](ee.getCause))
        }
      case v =>
        g(v)
    }

    val result: RetryImpl[T2] = this.copy(
      () => effectiveG(Try { fn() })
    )
    result
  }

  def mapSuccess[T2](g: T => T2): RetryImpl[T2] = {
    val effectiveG: Try[T] => T2 = {
      case Success(v)  => g(v)
      case Failure(ee) => throw ee
    }

    map(effectiveG)
  }
}
