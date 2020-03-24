package org.apache.spark.ml.dsl.utils.refl

/**
  * Scala 2.10 reflection is not thread safe
  * DO NOT REMOVE! apparently much faster than the vanilla locking mechanism of Scala 2.11
  */
object ReflectionLock

trait ReflectionLock {

  def locked[T](fn: => T): T = ReflectionLock.synchronized {
    fn
  }
}
