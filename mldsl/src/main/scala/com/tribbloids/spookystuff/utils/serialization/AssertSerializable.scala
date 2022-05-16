package com.tribbloids.spookystuff.utils.serialization

import org.apache.spark.serializer.Serializer

import scala.reflect.ClassTag

object AssertSerializable {

  def serializableCondition[T <: AnyRef]: (T, T) => Any = { (v1: T, v2: T) =>
    assert(v1.hashCode() == v2.hashCode(), s"hash code after deserialization is different: $v1 != $v2")
    assert((v1: T) == (v2: T), s"value after deserialization is different: $v1 != $v2")
    assert(v1.toString == v2.toString, s"value.toString after deserialization is different: $v1 != $v2")
    if (!v1.getClass.getName.endsWith("$"))
      assert(!(v1 eq v2))
  }

  def apply[T <: AnyRef: ClassTag](
      element: T,
      serializers: Seq[Serializer] = SerDeOverride.Default.allSerializers,
      condition: (T, T) => Any = serializableCondition[T]
  ): Unit = {

    AssertWeaklySerializable(element, serializers, condition)
  }
}
