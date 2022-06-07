package org.apache.spark.ml.dsl.utils.refl

/**
  * Scala 2.10 reflection is not thread safe apparently faster than the vanilla locking mechanism of Scala 2.11
  */
// TODO: remove, thread safety issue is long gone
object ReflectionLock

trait ReflectionLock {

  def locked[T](fn: => T): T = ReflectionLock.synchronized {
    fn
  }
}
