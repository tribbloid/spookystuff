package com.tribbloids.spookystuff.relay.json

import ai.acyclic.prover.commons.spark.serialization.SerializerEnv
import org.json4s.{Formats, JString, JValue, Serializer, TypeInfo}
import org.slf4j.LoggerFactory

import java.nio.ByteBuffer
import java.util.Base64

object ToBinaryCompat {

  // fallback mechanism that works for any java object
  abstract class SparkBinarySerDe(
      sparkSerializer: org.apache.spark.serializer.Serializer
  ) extends Serializer[Any] {

//    val VID: Long = -47597349821L

    @transient lazy val base64: (Base64.Encoder, Base64.Decoder) = {
      Base64.getEncoder -> Base64.getDecoder
    }

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
            val bytes = base64._2.decode(str.trim)

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

            val str = base64._1.encodeToString(buffer.array())

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

  object SparkBinarySerDe {

    object JVM extends SparkBinarySerDe(SerializerEnv.Default.javaSerializer)

    object Kryo extends SparkBinarySerDe(SerializerEnv.Default.kryoSerializer)
  }
}
