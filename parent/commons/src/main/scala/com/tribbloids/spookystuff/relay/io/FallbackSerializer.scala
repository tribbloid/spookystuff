package com.tribbloids.spookystuff.relay.io

import com.tribbloids.spookystuff.utils.serialization.SerializerEnv
import org.apache.spark.ml.dsl.utils.EncodedBinaryMagnet.Base64
import org.json4s._
import org.slf4j.LoggerFactory

import java.nio.ByteBuffer

// fallback mechanism that works for any java object
abstract class FallbackSerializer(
    sparkSerializer: org.apache.spark.serializer.Serializer
) extends Serializer[Any] {

  val VID: Long = -47597349821L

  def deserialize(
      implicit
      format: Formats
  ): PartialFunction[(TypeInfo, JValue), Any] = {
    Function.unlift {
      case (_, JString(str)) =>
        LoggerFactory
          .getLogger(this.getClass)
          .debug(
            s"JSON === [${this.getClass.getSimpleName}] ==> Object"
          )
        try {
          val bytes = Base64.fromStr(str.trim)

          val ser = sparkSerializer.newInstance()

//          implicit val ctg = ClassTag(ti.clazz)
          val de = ser.deserialize[Any](
            ByteBuffer.wrap(bytes)
          )
          Some(de)
        } catch {
          case _: Exception =>
            None
        }
      case _ =>
        None
    }
  }

  def serialize(
      implicit
      format: Formats
  ): PartialFunction[Any, JValue] = {
    Function.unlift {
      case v: Serializable =>
        LoggerFactory
          .getLogger(this.getClass)
          .debug(
            s"Object === [${this.getClass.getSimpleName}] ==> JSON"
          )
        //        try {
        //          val result = Extraction.decompose(v)(format)
        //          Some(result)
        //        }
        //        catch {
        //          case e: MappingException =>
        try {
          val ser = sparkSerializer.newInstance()
          val buffer: ByteBuffer = ser.serialize(v)

          val str = Base64(buffer.array()).asBase64Str

          Some(JString(str))
        } catch {
          case _: Exception =>
            None
        }
      //        }

      case _ =>
        None
    }
  }
}

object FallbackSerializer extends FallbackSerializer(SerializerEnv.Default.javaSerializer)
