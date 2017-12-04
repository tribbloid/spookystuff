package org.apache.spark.ml.dsl.utils.refl

/**
  * Scala 2.10 reflection is not thread safe
  */
object ReflectionLock

trait ReflectionLock {

  def locked[T](fn: =>T) = ReflectionLock.synchronized {
    fn
  }
}
