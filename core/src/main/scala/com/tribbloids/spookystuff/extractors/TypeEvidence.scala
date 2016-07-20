package com.tribbloids.spookystuff.extractors

import org.apache.spark.sql.TypeUtils
import org.apache.spark.sql.types.DataType

/**
  * Persisted between
  */
object TypeEvidence {

  import TypeUtils.Implicits._

  def apply(catalystType: DataType): TypeEvidence = TypeEvidence(
    catalystType,
    None
  )

  def apply(scalaType: TypeTag[_]): TypeEvidence = TypeEvidence(
    TypeUtils.catalystTypeFor(scalaType),
    Some(scalaType)
  )

  def fromInstance(obj: Any): TypeEvidence = {
    val clazz = obj.getClass
    apply(clazz.toTypeTag)
  }
}

case class TypeEvidence(
                         catalystType: DataType,
                         declaredScalaTypeOpt: Option[TypeTag[_]]
                       ) {

  //TODO: declared as private for being too complex, will be enabled later
  private def scalaTypes: Seq[TypeTag[_]] = {
    val opt = declaredScalaTypeOpt.map(v => Seq(v))
    opt match {
      case Some(ss) => ss
      case None =>
        TypeUtils.scalaTypesFor(catalystType)
    }
  }

  def baseScalaType: TypeTag[_] = {
    declaredScalaTypeOpt
      .getOrElse {
        TypeUtils.baseScalaTypeFor(catalystType)
      }
  }
}