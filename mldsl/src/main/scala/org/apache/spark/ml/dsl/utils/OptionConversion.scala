package org.apache.spark.ml.dsl.utils

import scala.language.implicitConversions

trait OptionConversion {

  implicit def box[T](v: T): Option[T] = Option(v)
}
