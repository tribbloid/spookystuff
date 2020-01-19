package org.apache.spark.ml.dsl.utils

import com.tribbloids.spookystuff.utils.IDMixin

import scala.language.implicitConversions

//TODO: merge into NullSafe
//TODO: use AnyVal to minimise overhead
//TODO: need thread safety test
/**
  * similar to lazy val, but cached value can be peeked & overwritten from outside
  * thread safe and value only initialized once
  */
class LazyVar[T](
    fn: => T
) extends Serializable
    with IDMixin {

  val cached: T ? Var = None

  def peek: Option[T] = cached.asOption

  def get: T = peek.getOrElse {
    this.synchronized {

      peek.getOrElse(regenerate)
    }
  }

  def regenerate: T = {
    val result = fn
    set(result)
    result
  }

  def set(v: T): Unit = {
    cached := v
  }

  def isCached: Boolean = cached.asOption.nonEmpty

  override def _id: Any = get

  override def toString: String = get.toString
}

object LazyVar {

  implicit def unbox[T](v: LazyVar[T]): T = v.get

  def apply[T](fn: => T) = new LazyVar[T](fn)
}
