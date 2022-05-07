package org.apache.spark.ml.dsl.utils.messaging

trait MessageReaderLevel1 {

  implicit def fromMF[T](
      implicit
      mf: Manifest[T]
  ): MessageReader[T] = new MessageReader[T]()(mf)
}
