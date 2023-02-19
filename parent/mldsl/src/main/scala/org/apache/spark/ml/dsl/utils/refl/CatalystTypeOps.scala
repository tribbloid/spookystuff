package org.apache.spark.ml.dsl.utils.refl

import org.apache.spark.sql.types._

import java.sql.{Date, Timestamp}
import scala.collection.Map
import scala.language.{existentials, implicitConversions}

case class CatalystTypeOps(dataType: DataType) {

  import TypeMagnet.universe._

  // CatalystType => ScalaType
  // used in ReflectionMixin to determine the exact function to:
  // 1. convert data from CatalystType to canonical Scala Type (and obtain its TypeTag)
  // 2. use the obtained TypeTag to get the specific function implementation and applies to the canonic Scala Type data.
  // 3. get the output TypeTag of the function, use it to generate the output DataType of the new Extraction.
  // TODO: should rely on spark impls including Literal, ScalaReflection & CatalystTypeConverters
  def getTypeTagOpt: Option[TypeTag[_]] = {

    dataType match {
      case NullType =>
        Some(TypeTag.Null)
      case st: CatalystTypeMixin[_] =>
        Some(st.self.asTypeTag)
      case t if CatalystTypeOps.atomicTypeMap.contains(t) =>
        CatalystTypeOps.atomicTypeMap.get(t)
      case ArrayType(inner, _) =>
        val innerTagOpt = CatalystTypeOps(inner).getTypeTagOpt
        innerTagOpt.map {
          case at: TypeTag[a] =>
            implicit val att: TypeTag[a] = at
            typeTag[Array[a]]
        }
      case MapType(key, value, _) =>
        val keyTag = CatalystTypeOps(key).getTypeTagOpt
        val valueTag = CatalystTypeOps(value).getTypeTagOpt
        val pairs = (keyTag, valueTag) match {
          case (Some(kt), Some(vt)) => Some(kt -> vt)
          case _                    => None
        }

        pairs.map { pair =>
          (pair._1, pair._2) match {
            case (ttg1: TypeTag[a], ttg2: TypeTag[b]) =>
              implicit val t1: TypeTag[a] = ttg1
              implicit val t2: TypeTag[b] = ttg2
              typeTag[Map[a, b]]
          }
        }
      case StructType(fields) =>
        fields.length match {
          case 1 => Some(typeTag[Product1[Any]])
          case 2 => Some(typeTag[Product2[Any, Any]])
          case 3 => Some(typeTag[Product3[Any, Any, Any]])
          case 4 => Some(typeTag[Product4[Any, Any, Any, Any]])
          case 5 => Some(typeTag[Product5[Any, Any, Any, Any, Any]])
          // TODO: keep going for this, OR find more general solution, using shapeless?
          case _ => None
        }
      case _ =>
        None
    }
  }

  def typeTag_wild: TypeTag[_] = {
    getTypeTagOpt.getOrElse {
      throw new UnsupportedOperationException(
        s"cannot convert Catalyst type $dataType to Scala type: TypeTag=${getTypeTagOpt}"
      )
    }
  }

  @transient lazy val reified: DataType = {
    val result = UnreifiedObjectType.reify(dataType)
    result
  }

  class AsTypeMagnet[T]() extends TypeMagnet[T] {

    override def _typeTag: TypeTag[T] = typeTag_wild.asInstanceOf[TypeTag[T]]
  }

  def magnet[T]: AsTypeMagnet[T] = new AsTypeMagnet[T]

//  @transient lazy val asClassTag: ClassTag[_] = {
//
//    dataType match {
//      case v: CatalystTypeMixin[_] =>
//        v.self.asClassTag
//      case _ =>
//        val clazz = org.apache.spark.sql.catalyst.expressions.Literal.default(dataType).eval().getClass
//        ClassTag(clazz)
//    }
//  }

//  def asClass: Class[_] = asClassTag.runtimeClass

  def unboxArrayOrMap: DataType = {
    _unboxArrayOrMapOpt
      .orElse(
        CatalystTypeOps(reified)._unboxArrayOrMapOpt
      )
      .getOrElse(
        throw new UnsupportedOperationException(s"Type $dataType is not an Array")
      )
  }

  private[utils] def _unboxArrayOrMapOpt: Option[DataType] = {
    dataType match {
      case ArrayType(boxed, _) =>
        Some(boxed)
      case MapType(keyType, valueType, valueContainsNull) =>
        Some(
          StructType(
            Array(
              StructField("_1", keyType),
              StructField("_2", valueType, valueContainsNull)
            )
          )
        )
      case _ =>
        None
    }
  }

  def filterArray: Option[DataType] = {
    if (reified.isInstanceOf[ArrayType])
      Some(dataType)
    else
      None
  }

  def asArray: DataType = {
    filterArray.getOrElse {
      ArrayType(dataType)
    }
  }

  def ensureArray: DataType = {
    filterArray.getOrElse {
      throw new UnsupportedOperationException(s"Type $dataType is not an Array")
    }
  }

  def =~=(another: DataType): Boolean = {
    val result = (dataType eq another) ||
      (dataType == another) ||
      (reified == CatalystTypeOps(another).reified)

    result
  }

  def should_=~=(another: DataType): Unit = {
    val result = =~=(another)
    assert(
      result,
      s"""
           |Type not equal:
           |LEFT:  $dataType -> ${reified}
           |RIGHT: $another -> ${CatalystTypeOps(another).reified}
        """.stripMargin
    )
  }
}

object CatalystTypeOps {

  import TypeMagnet.universe._

  trait ImplicitMixin {

    implicit def convert(v: DataType): CatalystTypeOps = CatalystTypeOps(v)
  }

  lazy val atomicExamples: Seq[(Any, TypeTag[_])] = {

    implicit def pairFor[T: TypeTag](v: T): (T, TypeTag[T]) = {
      v -> TypeUtils.summon[T](v)
    }

    val result = Seq[(Any, TypeTag[_])](
      Array(0: Byte),
      false: Boolean,
      new Date(0),
      new Timestamp(0),
      0.0: Double,
      0: Float,
      0: Byte,
      0: Int,
      0L: Long,
      0: Short,
      "a": String
    )
    result
  }

  lazy val atomicTypePairs: Seq[(DataType, TypeTag[_])] = atomicExamples.map { v =>
    ToCatalyst(TypeMagnet.FromTypeTag(v._2)).tryReify.get -> v._2
  }

  lazy val atomicTypeMap: Map[DataType, TypeTag[_]] = {
    Map(atomicTypePairs: _*)
  }

}
