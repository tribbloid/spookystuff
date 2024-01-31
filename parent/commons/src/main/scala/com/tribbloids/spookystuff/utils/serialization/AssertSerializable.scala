package com.tribbloids.spookystuff.utils.serialization

import org.apache.spark.serializer.Serializer

import scala.reflect.ClassTag

object AssertSerializable {

  def serializableCondition[T <: Any]: (T, T) => Any = { (v1: T, v2: T) =>
    assert(v1.hashCode() == v2.hashCode(), s"hash code after deserialization is different: $v1 != $v2")
    assert((v1: T) == (v2: T), s"value after deserialization is different: $v1 != $v2")
    assert(v1.toString == v2.toString, s"value.toString after deserialization is different: $v1 != $v2")
    //    (v1, v2) match {
    //      case (_1: AnyRef, _2: AnyRef) =>
    //        if (!v1.getClass.getName.endsWith("$")) {
    //        TODO: remove, kryo serializer automatically interns https://stackoverflow.com/questions/10578984/what-is-java-string-interning
    //
    //          assert(!(_1 eq _2))
    //        }
    //    }
  }

  def apply[T <: Any: ClassTag](
      element: T,
      serializers: Seq[Serializer] = SerializerEnv.Default.allSerializers,
      condition: (T, T) => Any = serializableCondition[T]
  ): Unit = {

    AssertWeaklySerializable(element, serializers, condition)
  }
}
