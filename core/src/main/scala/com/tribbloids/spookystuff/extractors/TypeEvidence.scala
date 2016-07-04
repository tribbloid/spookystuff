package com.tribbloids.spookystuff.extractors

import org.apache.spark.sql.TypeUtils
import org.apache.spark.sql.types.DataType

/**
  * Created by peng on 25/06/16.
  */
object TypeEvidence {

  def apply(catalystType: DataType): TypeEvidence = TypeEvidence(
    catalystType,
    None
  )

  def apply(scalaType: TypeTag[_]): TypeEvidence = TypeEvidence(
    TypeUtils.catalystTypeOrDefault()(scalaType),
    Some(scalaType)
  )
}

case class TypeEvidence(
                         catalystType: DataType,
                         _scalaTypeOpt: Option[TypeTag[_]]
                       ) {

  def scalaTypeOpt: Option[TypeTag[_]] = {
    val effective: Option[TypeTag[_]] = _scalaTypeOpt
      .map(v => Option(v))
      .getOrElse{
        val default = TypeUtils.scalaTypeFor(catalystType)
        default
      }
    effective
  }
}