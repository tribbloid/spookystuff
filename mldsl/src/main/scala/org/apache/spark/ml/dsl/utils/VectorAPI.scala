package org.apache.spark.ml.dsl.utils

import org.apache.spark.ml.linalg._

import scala.language.implicitConversions

@Deprecated
trait VectorAPI {

  def encode: Vector
}

object VectorAPI {

  def decode[T <: VectorAPI: Decoder](vector: Vector): T = {
    implicitly[Decoder[T]].decode(vector)
  }

  trait Decoder[T <: VectorAPI] {
    implicit final def decode(vector: Vector): T = _decode(vector)

    def _decode(vector: Vector): T
  }
}
