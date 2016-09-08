package org.apache.spark.ml.dsl.utils

import java.nio.ByteBuffer

import org.apache.spark.SparkConf
import org.apache.spark.serializer.JavaSerializer
import org.json4s._

import scala.reflect.ClassTag

// fallback mechanism that works for any java object
abstract class BinaryJSONSerializer(
                                     sparkSerializer: org.apache.spark.serializer.Serializer = { //TODO: kryo is better
                                     val conf = new SparkConf()
                                       new JavaSerializer(conf)
                                     }
                                   ) extends Serializer[Any] {

  val VID = -47597349821L

  def deserialize(implicit format: Formats): PartialFunction[(TypeInfo, JValue), Any] = {

    case (ti, JString(str)) =>
      val bytes = new Base64Wrapper(str.trim).asBytes

      val ser = sparkSerializer.newInstance()

      implicit val ctg = ClassTag(ti.clazz)
      val de = ser.deserialize[Any](
        ByteBuffer.wrap(bytes)
      )

      de
  }

  def serialize(implicit format: Formats): PartialFunction[Any, JValue] = {
    case value =>

      val serializableValue = value.asInstanceOf[Any @SerialVersionUID(VID) with Serializable]

      val ser = sparkSerializer.newInstance()
      val buffer: ByteBuffer = ser.serialize(serializableValue)

      val str = new Base64Wrapper(buffer.array()).asBase64Str

      JString(str)
  }
}

object FallbackJSONSerializer extends BinaryJSONSerializer()