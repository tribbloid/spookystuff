package org.apache.spark.ml.dsl.utils

import org.apache.spark.ml.param.Params
import org.apache.spark.ml.util.DefaultParamsReader.Metadata
import org.apache.spark.ml.util.{DefaultParamsReader, MLReadable, MLReader}
import org.apache.spark.util.Utils
import org.json4s.JsonAST.JObject
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.json4s.{Formats, Serializer}

import scala.xml.{NodeSeq, XML}

//TODO: merge into StructRelay
trait XMLReaderMixin[T] {

  implicit def mf: Manifest[T]

  def extraSer: Seq[Serializer[_]] = Nil

  implicit def format: Formats = Xml.defaultFormats

  def fromJson(json: String): T = {
    Serialization.read[T](json)
  }

  def fromXml(xml: NodeSeq): T = {
    val json = Xml.toJson(xml)

    json.extract[T]
  }

  def fromXmlStr(xmlStr: String): T = {
    val xml = XML.loadString(xmlStr)

    fromXml(xml)
  }
}

class XMLStageReader[T <: Params](implicit val mf: Manifest[T]) extends MLReader[T] with XMLReaderMixin[T] {

  override def load(path: String): T = {
    val metadata = DefaultParamsReader.loadMetadata(path, sc)
    val cls = Utils.classForName(metadata.className)
    val instance =
      cls.getConstructor(classOf[String]).newInstance(metadata.uid).asInstanceOf[T]
    this.getAndSetParams(instance, metadata)
    instance
  }

  /**
    * Extract Params from metadata, and set them in the instance.
    * This works if all Params implement org.apache.spark.ml.param.Param.jsonDecode().
    */
  def getAndSetParams(instance: T, metadata: Metadata): Unit = {
    metadata.params match {
      case JObject(pairs) =>
        pairs.foreach {
          case (paramName, jValue) =>
            val param = instance.getParam(paramName)
            val value = param.jsonDecode(compact(render(jValue)))
            instance.set(param, value)
        }
      case _ =>
        throw new IllegalArgumentException(s"Cannot recognize JSON metadata: ${metadata.metadataJson}.")
    }
  }
}

class XMLStageReadable[T <: Params](implicit val mf: Manifest[T]) extends MLReadable[T] {

  object Reader extends XMLStageReader[T]

  @org.apache.spark.annotation.Since("1.6.0")
  override def read: MLReader[T] = Reader
}
