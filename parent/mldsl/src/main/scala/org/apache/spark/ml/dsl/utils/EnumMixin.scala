package org.apache.spark.ml.dsl.utils

trait EnumMixin extends ObjectSimpleNameMixin with Serializable {

  override def toString: String = objectSimpleName
}
