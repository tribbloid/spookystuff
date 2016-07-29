package com.tribbloids.spookystuff.utils

import com.tribbloids.spookystuff.utils.ImplicitUtils._
import org.apache.spark.sql.catalyst.ScalaReflection.universe._
import org.apache.spark.sql.types._

trait ScalaType extends DataType with Serializable with IDMixin {

  @transient val ttg: TypeTag[_]

  def catalystTypeOpt: Option[DataType] = ttg.catalystTypeOpt

  // if catalystTypeOpt is unable to resolve to the same typeTag, do not
//  val nonDegradingCatalystType: DataType = {
//    val ttOpt = catalystTypeOpt
//
//    ttOpt.flatMap{
//      tt =>
//        tt.scalaTypeOpt.flatMap{
//          reconstructedTTg =>
//            if (reconstructedTTg == ttg) Some(tt)
//            else None
//        }
//    }
//      .getOrElse (
//        this
//      )
//  }

  def catalystType = catalystTypeOpt.getOrElse {
    this
  }

  // see [SPARK-8647], this achieves the needed constant hash code without declaring singleton
  //TODO: this is not accurate due to type erasure, need a better way to handle both type erasure & type alias
  override val _id = {
    "" + ttg.toClass + "/" + catalystTypeOpt
  }

  override def toString = typeName
}

/**
  * Can only exist in DataRowSchema & extractor to remember ScalaType
  * Cannot be used in DataFrame schema
  */
trait UnreifiedType extends DataType

class UnreifiedScalaType(@transient val ttg: TypeTag[_]) extends ScalaType with UnreifiedType {

  override def defaultSize: Int = 0

  override def asNullable: DataType = this

  def reify: DataType = catalystType

  override val typeName: String = "(unreified) " + ttg.tpe
}

object UnreifiedScalaType {

  def apply[T](implicit ttg: TypeTag[T]): DataType = {
    ttg match {
      case TypeTag.Null => NullType
      case _ => new UnreifiedScalaType(ttg)
    }
  }

  def fromInstance[T](obj: T): DataType = {
    val clazz: Class[_ <: T] = obj.getClass
    apply(clazz.toTypeTag)
  }

  def reify(tt: DataType): DataType = {
    tt match {
      case udt: UnreifiedScalaType => udt.reify
      case ArrayType(v, n) =>
        ArrayType(reify(v), n)
      case StructType(fields) =>
        StructType(
          fields.map {
            ff =>
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

/**
  *  A Scala TypeTag-based UDT, by default it doesn't compress object
  *  ideally it should compress object into InternalRow.
  */
//TODO: UserDefinedType API will be deprecated in Spark 2.0.0, must figure a way out!
abstract class ScalaUDT[T](@transient implicit val ttg: TypeTag[T]) extends UserDefinedType[T] with ScalaType {

  def sqlType: DataType = catalystType

  //should convert to internal Row.
  override def serialize(obj: Any): Any = obj

  override def deserialize(datum: Any): T = datum match {
    case a: T => a
  }

  override val userClass: Class[T] = {
    ttg.toClass
  }
}