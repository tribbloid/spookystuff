package org.apache.spark.ml.dsl.utils.refl

import org.apache.spark.sql.types.DataType

case class ToCatalyst[T](m: TypeMagnet[T]) {

  @transient lazy val tryReify: scala.util.Try[DataType] = {
    TypeUtils
      .tryCatalystTypeFor(m.asTypeTag)
  }

  def reify: DataType = tryReify.get

  def asCatalystType: DataType = tryReify.getOrElse {
    new UnreifiedObjectType[T]()(m)
  }
}
