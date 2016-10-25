package org.apache.spark.ml.dsl.utils

import java.nio.ByteBuffer

import org.apache.spark.SparkConf
import org.apache.spark.serializer.JavaSerializer
import org.json4s._
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag

// fallback mechanism that works for any java object
abstract class FallbackSerializer(
                                     sparkSerializer: org.apache.spark.serializer.Serializer = { //TODO: kryo is better
                                     val conf = new SparkConf()
                                       new JavaSerializer(conf)
                                     }
                                   ) extends Serializer[Any] {

  val VID = -47597349821L

  def deserialize(implicit format: Formats): PartialFunction[(TypeInfo, JValue), Any] = {
    Function.unlift{
      case (ti, JString(str)) =>
        LoggerFactory.getLogger(this.getClass).info(
          s"JSON === [${this.getClass.getSimpleName}] ==> Object"
        )
        try {
          val bytes = new Base64Wrapper(str.trim).asBytes

          val ser = sparkSerializer.newInstance()

          implicit val ctg = ClassTag(ti.clazz)
          val de = ser.deserialize[Any](
            ByteBuffer.wrap(bytes)
          )
          Some(de)
        }
        catch {
          case e: Exception =>
            None
        }
      case _ =>
        None
    }
  }

  def serialize(implicit format: Formats): PartialFunction[Any, JValue] = {
    Function.unlift {
      case v: Serializable =>
        LoggerFactory.getLogger(this.getClass).info(
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

              val str = new Base64Wrapper(buffer.array()).asBase64Str

              Some(JString(str))
            }
            catch {
              case e: Throwable =>
                None
            }
//        }

      case _ =>
        None
    }
  }
}

object FallbackJSONSerializer extends FallbackSerializer()