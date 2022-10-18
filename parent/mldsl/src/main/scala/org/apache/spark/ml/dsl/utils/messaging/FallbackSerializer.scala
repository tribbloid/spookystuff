package org.apache.spark.ml.dsl.utils.messaging

import org.apache.spark.ml.dsl.utils.DSLUtils
import org.apache.spark.ml.dsl.utils.EncodedBinaryMagnet.Base64
import org.json4s._
import org.slf4j.LoggerFactory

import java.nio.ByteBuffer
import scala.reflect.ClassTag

// fallback mechanism that works for any java object
abstract class FallbackSerializer(
    sparkSerializer: org.apache.spark.serializer.Serializer = DSLUtils.defaultJavaSerializer
) extends Serializer[Any] {

  val VID = -47597349821L

  def deserialize(
      implicit
      format: Formats
  ): PartialFunction[(TypeInfo, JValue), Any] = {
    Function.unlift {
      case (ti, JString(str)) =>
        LoggerFactory
          .getLogger(this.getClass)
          .debug(
            s"JSON === [${this.getClass.getSimpleName}] ==> Object"
          )
        try {
          val bytes = Base64.fromStr(str.trim)

          val ser = sparkSerializer.newInstance()

          implicit val ctg = ClassTag(ti.clazz)
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

object FallbackSerializer extends FallbackSerializer(DSLUtils.defaultJavaSerializer)
