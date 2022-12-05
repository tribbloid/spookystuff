package org.apache.spark.ml.dsl.utils.messaging

trait MessageReaderLevel1 {

  implicit def fallback[T]: MessageReader[T] = new MessageReader[T]()
}
