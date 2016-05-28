package com.tribbloids.spookystuff.utils

import com.tribbloids.spookystuff.actions.Action
import com.tribbloids.spookystuff.doc.{Fetched, Unstructured}
import org.apache.spark.sql.types.{DataType, UserDefinedType}

import scala.reflect.ClassTag

/**
  * Makeshift UDT that serialize object directly, very memory-inefficient.
  * DOES NOT support toJSON
  */
class AnyUDT[T <: Serializable: ClassTag] extends UserDefinedType[T] {

  override def sqlType: DataType = this.getClass.getConstructor().newInstance()

  override def serialize(obj: Any): Any = obj

  override def deserialize(datum: Any): T = datum match {
    case a: T => a
  }

  override def userClass: Class[T] = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]
}

class ActionUDT extends AnyUDT[Action]

class UnstructuredUDT extends AnyUDT[Unstructured]

class FetchedUDT extends AnyUDT[Fetched]