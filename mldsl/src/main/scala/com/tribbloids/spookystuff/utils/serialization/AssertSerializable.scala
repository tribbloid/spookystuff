package com.tribbloids.spookystuff.utils.serialization

import java.nio.ByteBuffer

import org.apache.spark.serializer.Serializer

import scala.reflect.ClassTag

object AssertSerializable {

  def apply[T <: AnyRef: ClassTag](
      element: T,
      serializers: Seq[Serializer] = SerBox.serializers,
      condition: (T, T) => Any = { (v1: T, v2: T) =>
        assert((v1: T) == (v2: T))
        assert(v1.toString == v2.toString)
        if (!v1.getClass.getCanonicalName.endsWith("$"))
          assert(!(v1 eq v2))
      }
  ): Unit = {

    AssertWeaklySerializable(element, serializers, condition)
  }
}

case class AssertWeaklySerializable[T <: AnyRef: ClassTag](
    element: T,
    serializers: Seq[Serializer] = SerBox.serializers,
    condition: (T, T) => Any = { (v1: T, v2: T) =>
      true
    }
) {

  serializers.foreach { ser =>
    val serInstance = ser.newInstance()
    val binary: ByteBuffer = serInstance.serialize(element)
    assert(binary.array().toSeq.exists(_.toInt > 8)) //min object overhead length
    val element2 = serInstance.deserialize[T](binary)
    //      assert(!element.eq(element2))
    condition(element, element2)
  }
}
