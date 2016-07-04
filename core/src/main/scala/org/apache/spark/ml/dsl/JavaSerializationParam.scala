package org.apache.spark.ml.dsl

import java.nio.ByteBuffer

import org.apache.spark.SparkConf
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.dsl.utils.Base64Wrapper
import org.apache.spark.ml.param.{Param, ParamPair}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.serializer.JavaSerializer
import org.json4s.jackson.JsonMethods._
import org.json4s.{DefaultFormats, _}

import scala.reflect.ClassTag

object JavaSerializationParam {

  final val vid = -47597349821L
}

/**
  * :: DeveloperApi ::
  * Specialized version of Param[Any] for Java.
  */
@DeveloperApi
class JavaSerializationParam[T: ClassTag](parent: String, name: String, doc: String, isValid: T => Boolean)
  extends Param[T](parent, name, doc, isValid) {

  def this(parent: String, name: String, doc: String) =
    this(parent, name, doc, (_: T) => true)

  def this(parent: Identifiable, name: String, doc: String, isValid: T => Boolean) =
    this(parent.uid, name, doc, isValid)

  def this(parent: Identifiable, name: String, doc: String) = this(parent.uid, name, doc)

  //  val serializer = SparkEnv.get.serializer
  val serializer: JavaSerializer = {
    val conf = new SparkConf()
    new JavaSerializer(conf)
  }

  /** Creates a param pair with the given value (for Java). */
  override def w(value: T): ParamPair[T] = super.w(value)

  override def jsonEncode(value: T): String = {

    val serializableValue = value.asInstanceOf[T @SerialVersionUID(JavaSerializationParam.vid) with Serializable]

    val ser = serializer.newInstance()
    val buffer: ByteBuffer = ser.serialize(serializableValue)

    val str = new Base64Wrapper(buffer.array()).asBase64Str

    compact(render(JString(str)))
  }

  override def jsonDecode(json: String): T = {
    implicit val formats = DefaultFormats

    val str = parse(json).extract[String]
    val bytes = new Base64Wrapper(str.trim).asBytes

    val ser = serializer.newInstance()
    val de = ser.deserialize[T](
      ByteBuffer.wrap(bytes)
    )

    de
  }
}