package org.apache.spark.ml.dsl.utils.refl

import org.apache.spark.sql.types._

/**
  * Can only exist in DataRowSchema & extractor to remember ScalaType Not allowed to be used in DataFrame schema
  * WARNING: this cannot be completely superceded by ScalaUDT the later has to be abstract and not generic, and
  * discoverable through annotation
  */
class UnknownDT[T]()(
    implicit
    val typeMagnet: TypeMagnet[T]
) extends ObjectType(typeMagnet.asClass)
    with CustomDT[T] {

  override lazy val productPrefix: String = "Unreified"
}

object UnknownDT {

  def reify(tt: DataType): DataType = {
    tt match {
      case udt: UnknownDT[_] =>
        udt.typeMagnet.asCatalystType
      case ArrayType(v, n) =>
        ArrayType(reify(v), n)
      case StructType(fields) =>
        StructType(
          fields.map { ff =>
            ff.copy(
              dataType = reify(ff.dataType)
            )
          }
        )
      case MapType(k, v, n) =>
        MapType(reify(k), reify(v), n)
      case _ => tt
    }
  }
}
