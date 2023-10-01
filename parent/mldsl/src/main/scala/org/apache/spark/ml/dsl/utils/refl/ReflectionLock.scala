package org.apache.spark.ml.dsl.utils.refl

object ReflectionLock

trait ReflectionLock {

  // only a dummy function, thread safety issue is long gone TODO: remove
  @inline def locked[T](fn: => T): T = {
    fn
  }
}
