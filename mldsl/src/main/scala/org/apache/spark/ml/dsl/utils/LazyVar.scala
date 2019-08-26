package org.apache.spark.ml.dsl.utils

import scala.language.implicitConversions

//TODO: merge into NullSafe
//TODO: use AnyVal to minimise overhead
//TODO: need thread safety test
/**
  * similar to lazy val, but cached value can be overwritten from outside
  */
class LazyVar[T](
    fn: => T
) extends Serializable {

  val cached: T ? Var = None

  private def opt = cached.asOption

  def value: T = opt.getOrElse {
    this.synchronized {

      opt.getOrElse {
        val result = fn
        :=(result)
        result
      }
    }
  }

  def :=(v: T): Unit = {
    cached := v
  }

  def isCached: Boolean = cached.asOption.nonEmpty
}

object LazyVar {

  implicit def unbox[T](v: LazyVar[T]): T = v.value

  def apply[T](fn: => T) = new LazyVar[T](fn)
}
