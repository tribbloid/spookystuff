package com.tribbloids.spookystuff.utils

import com.tribbloids.spookystuff.extractors.TypeTag
import org.apache.spark.sql.types.{DataType, UserDefinedType}

import scala.reflect.ClassTag

/**
  * Makeshift UDT that serialize object directly, very memory-inefficient.
  * DOES NOT support toJSON
  */
class AnyUDT[T <: Serializable](@transient implicit val ttg: TypeTag[T]) extends UserDefinedType[T] with IdentifierMixin {

  import org.apache.spark.sql.TypeUtils.Implicits._

  implicit def ctg: ClassTag[T] = ttg.toClassTag

//  override def sqlType: DataType = this.getClass.getConstructor().newInstance()
  override def sqlType: DataType = this

  override def serialize(obj: Any): Any = obj

  override def deserialize(datum: Any): T = datum match {
    case a: T => a
  }

  override def userClass: Class[T] = ttg.toClass

  // see [SPARK-8647], this achieves the needed constant hash code without constant no.
  override def _id: Any = ttg.toClass.getCanonicalName
}