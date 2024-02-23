package com.tribbloids.spookystuff.commons.serialization

import com.tribbloids.spookystuff.commons.TreeThrowable
import org.apache.spark.serializer.Serializer

import java.io.NotSerializableException
import java.nio.ByteBuffer
import scala.reflect.ClassTag
import scala.util.Try

case class AssertWeaklySerializable[T <: Any: ClassTag](
    element: T,
    serializers: Seq[Serializer] = SerializerEnv.Default.allSerializers,
    condition: (T, T) => Any = { (_: T, _: T) =>
      true
    }
) {

  val trials: Seq[Try[Any]] = serializers.map { ser =>
    Try {
      val serInstance = ser.newInstance()
      val binary: ByteBuffer = serInstance.serialize(element)
      assert(binary.array().length >= 1)
      val element2 = serInstance.deserialize[T](binary)
      //      assert(!element.eq(element2))
      condition(element, element2)
    }
      .recover {
        case e: Throwable =>
          throw new AssertionError(
            s"cannot serialize with ${ser.getClass.getSimpleName}: ${e.getMessage}",
            e
          )
      }
  }

  try {
    TreeThrowable.&&&(
      trials
    )
  } catch {
    case e: Throwable =>
      throw new NotSerializableException().initCause(e)
  }
}
