package org.apache.spark.ml.dsl.utils.refl

trait RuntimeTypeOverride {

  def runtimeType: ScalaType[_]
}
