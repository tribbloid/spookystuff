package org.apache.spark.ml.dsl.utils.refl

import org.apache.spark.sql.catalyst.ScalaReflection.universe._
import org.apache.spark.sql.types._

/**
  * Can only exist in DataRowSchema & extractor to remember ScalaType Not allowed to be used in DataFrame schema
  * WARNING: this cannot be completely superceded by ScalaUDT the later has to be abstract and not generic, and
  * discoverable through annotation
  */
class UnreifiedObjectType[T]()(
    implicit
    val self: ScalaType[T]
) extends ObjectType(self.asClass)
    with ScalaType.CatalystTypeMixin[T] {

//  override def simpleString: String = "(unreified) " + ev.asType

//  override val typeName: String = "(unreified) " + ev.asType

  override lazy val productPrefix: String = "(unreified) " + super.productPrefix
}

object UnreifiedObjectType {

  def summon[T](
      implicit
      ttg: TypeTag[T]
  ): DataType = {
    if (ttg == TypeTag.Null) NullType
    else {
      new UnreifiedObjectType[T]()
    }
  }

  def forRuntimeInstance[T](obj: T): DataType = {
    val clazz: Class[_] = obj.getClass
    summon(ScalaType.FromClass(clazz).asTypeTag)
  }

  def reify(tt: DataType): DataType = {
    tt match {
      case udt: UnreifiedObjectType[_] =>
        udt.self.reify
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
