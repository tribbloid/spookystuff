package org.apache.spark.ml.dsl.utils.messaging.xml

import org.json4s.jackson.Serialization
import org.json4s.{Formats, Serializer}

import scala.xml.{NodeSeq, XML}

//TODO: merge into StructRelay
trait XMLReaderMixin[T] {

  implicit def mf: Manifest[T]

  def extraSer: Seq[Serializer[_]] = Nil

  implicit def format: Formats = XMLFormats.defaultFormats

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
