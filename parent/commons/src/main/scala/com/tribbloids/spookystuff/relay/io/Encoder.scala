package com.tribbloids.spookystuff.relay.io

import com.tribbloids.spookystuff.relay.xml.{XMLFormats, Xml}
import com.tribbloids.spookystuff.relay.{IR, Relay, TreeIR}
import org.json4s.JsonAST.JObject
import org.json4s.jackson.JsonMethods.*
import org.json4s.{Extraction, Formats, JValue}

import scala.xml.NodeSeq

case class Encoder[_IR <: IR](
    ir: _IR,
    formats: Formats = XMLFormats.defaultFormats
) extends Serializable {

//  def toMsg: ir.Body = ir.body

  // TODO: move into case class WFormats(.) and enable lazy val
  def toJValue(
      implicit
      formats: Formats = formats
  ): JValue = Extraction.decompose(ir.body)
  def compactJSON(
      implicit
      formats: Formats = formats
  ): String = compact(render(toJValue))
  def prettyJSON(
      implicit
      formats: Formats = formats
  ): String = pretty(render(toJValue))
  def toJSON(pretty: Boolean = true)(
      implicit
      formats: Formats = formats
  ): String = {
    if (pretty) prettyJSON(formats)
    else compactJSON(formats)
  }

  def toXMLNode(
      implicit
      formats: Formats = formats
  ): NodeSeq =
    Xml.toXml(JObject(ir.rootTag -> toJValue))
  def compactXML(
      implicit
      formats: Formats = formats
  ): String = toXMLNode.toString().replaceAllLiterally("\n", "")
  def prettyXML(
      implicit
      formats: Formats = formats
  ): String = XMLFormats.defaultXMLPrinter.formatNodes(toXMLNode)
  def toXMLStr(pretty: Boolean = true)(
      implicit
      formats: Formats = formats
  ): String = {
    if (pretty) prettyXML
    else compactXML
  }

  def cast[T](formats: Formats = formats)(reader: Relay[T]#DecoderView): T = {
    reader.fromJValue(toJValue(formats))
  }
}

object Encoder {

  trait HasEncoder[T] {

    implicit def toEncoder(v: T)(
        implicit
        relay: Relay[T]
    ): Encoder[relay.IR_>>] = relay.toEncoder_>>(v)
  }

  def forValue[V](v: V): Encoder[TreeIR.Leaf[V]] = {
    Encoder(
      TreeIR.leaf(v)
    )
  }

}
