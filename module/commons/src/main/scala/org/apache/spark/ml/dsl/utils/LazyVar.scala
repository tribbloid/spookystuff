package org.apache.spark.ml.dsl.utils

import ai.acyclic.prover.commons.multiverse.{CanEqual, Projection}
import ai.acyclic.prover.commons.util.Magnet.OptionMagnet

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
    with Projection.Equals {

  {
    canEqualProjections += CanEqual.Native.on(value)
  }

  @volatile protected var cached: OptionMagnet[T] = null.asInstanceOf[T]

  def peek: Option[T] = cached

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
    cached = v
  }

  def isCached: Boolean = cached.nonEmpty

  override def toString: String = value.toString
}

object LazyVar {

  implicit def unbox[T](v: LazyVar[T]): T = v.value

  def apply[T](fn: => T): LazyVar[T] = new LazyVar[T](fn)
}
