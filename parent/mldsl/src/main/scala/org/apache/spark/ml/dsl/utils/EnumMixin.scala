package org.apache.spark.ml.dsl.utils

trait EnumMixin extends ScalaNameMixin with Serializable {

  override def toString: String = objectSimpleName
}
