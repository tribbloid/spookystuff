package org.apache.spark.ml.dsl.utils

import com.tribbloids.spookystuff.utils.EqualBy

import scala.language.implicitConversions

//TODO: merge into NullSafe
//TODO: use AnyVal to minimise overhead
//TODO: need thread safety test
//TODO: merge into Cats Ref
/**
  * similar to lazy val, but cached value can be peeked & overwritten from outside thread safe and value only
  * initialized once
  */
class LazyVar[T](
    fn: => T
) extends Serializable
    with EqualBy {

  protected val cached: T `?` Var = None

  def peek: Option[T] = cached.asOption

  def value: T = peek.getOrElse {
    this.synchronized {

      peek.getOrElse(regenerate)
    }
  }

  def regenerate: T = {
    val result = fn
    :=(result)
    result
  }

  def :=(v: T): Unit = {
    cached := v
  }

  def isCached: Boolean = cached.asOption.nonEmpty

  override def _equalBy: Any = value

  override def toString: String = value.toString
}

object LazyVar {

  implicit def unbox[T](v: LazyVar[T]): T = v.value

  def apply[T](fn: => T): LazyVar[T] = new LazyVar[T](fn)
}
