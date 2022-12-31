package org.apache.spark.ml.dsl.utils.refl

trait RuntimeTypeTagged {
  // TODO: remove, not used at the moment

  def runtimeType: TypeMagnet[_]
}
