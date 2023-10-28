package org.apache.spark.ml.dsl.utils.refl

import org.apache.spark.sql.types.DataType

trait CustomDT[T] extends DataType with CatalystTypeOps.ImplicitMixin {

  def typeMagnet: TypeMagnet[T]
}
