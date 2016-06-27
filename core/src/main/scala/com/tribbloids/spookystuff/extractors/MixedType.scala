package com.tribbloids.spookystuff.extractors

import org.apache.spark.sql.TypeUtils
import org.apache.spark.sql.types.DataType

/**
  * Created by peng on 25/06/16.
  */
object MixedType {

  def apply(catalystType: DataType): MixedType = MixedType(
    catalystType,
    None
  )

  def apply(scalaType: TypeTag[_]): MixedType = MixedType(
    TypeUtils.catalystTypeOrDefault()(scalaType),
    Some(scalaType)
  )
}

case class MixedType(
                      catalystType: DataType,
                      scalaTypeOpt: Option[TypeTag[_]]
                      //TODO: add python type option?
                    ) {

  def scalaTypes: Set[TypeTag[_]] = {
//    val effective = scalaTypeOpt.map(v => Set(v))
//      .getOrElse{
//        val default = TypeUtils.scalaTypesFor(catalystType)
//        default
//      }
    null
  }
}