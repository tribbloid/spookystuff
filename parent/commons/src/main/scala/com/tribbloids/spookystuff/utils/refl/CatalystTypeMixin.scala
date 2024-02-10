package com.tribbloids.spookystuff.utils.refl

import org.apache.spark.sql.types.DataType

trait CatalystTypeMixin[T] extends DataType with CatalystTypeOps.ImplicitMixin {

  def self: TypeMagnet[T]
}
