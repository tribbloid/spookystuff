package org.apache.spark.ml.dsl.utils.messaging

import java.io.File

import org.apache.spark.ml.dsl.utils._
import org.json4s.JsonAST.JObject
import org.json4s.jackson.JsonMethods.{pretty, _}
import org.json4s.{Extraction, Formats, JValue}

import scala.language.implicitConversions
import scala.xml.NodeSeq

trait MessageAPI extends Serializable {

  def formats: Formats = Xml.defaultFormats

  def proto: Any = this

  def toJValue(implicit formats: Formats = formats): JValue = Extraction.decompose(proto)
  def compactJSON(implicit formats: Formats = formats): String = compact(render(toJValue))
  def prettyJSON(implicit formats: Formats = formats): String = pretty(render(toJValue))
  def toJSON(pretty: Boolean = true)(implicit formats: Formats = formats): String = {
    if (pretty) prettyJSON(formats)
    else compactJSON(formats)
  }

  def toXMLNode(implicit formats: Formats = formats): NodeSeq =
    Xml.toXml(JObject(proto.getClass.getSimpleName -> toJValue))
  def compactXML(implicit formats: Formats = formats): String = toXMLNode.toString().replaceAllLiterally("\n","")
  def prettyXML(implicit formats: Formats = formats): String = Xml.defaultXMLPrinter.formatNodes(toXMLNode)
  def toXMLStr(pretty: Boolean = true)(implicit formats: Formats = formats): String = {
    if (pretty) prettyXML
    else compactXML
  }

  def productStr(
                  start: String = "(",
                  sep: String = ",",
                  end: String = ")",
                  indentFn: Int => String = _ => "",
                  recursion: Int = 0
                ): String = {

    val msg = proto
    msg match {
      //      case v: Lit[_, _] =>
      //        v.toString
      case v: Product =>
        val strs = v.productIterator
          .map {
            case vv: MessageAPI =>
              vv.productStr(start, sep, end, indentFn, recursion + 1) //has verbose over
            case vv@ _ =>
              MessageView(vv).productStr(start, sep, end, indentFn, recursion + 1)
          }
          .map {
            str =>
              val indent = indentFn(recursion)
              indent + str
          }
        val concat = strs
          .mkString(v.productPrefix + start, sep, end)
        concat
      case _ =>
        "" + msg
    }
  }
  def toString_\\\ = this.productStr(File.separator, File.separator, File.separator)
  def toString_/:/ = this.productStr("/", "/", "/")

  def cast[T: MessageReader](implicit formats: Formats = formats) = {
    MessageReader._fromJValue[T](toJValue(formats))
  }
}

trait MessageAPI_<=>[Obj] extends MessageAPI {

  def toObject: Obj
}

case class MessageView(
                        value: Any,
                        override val formats: Formats = Xml.defaultFormats
                      ) extends MessageAPI {

  override def proto = value match {
    case v: MessageAPI => v.proto
    case _ => value
  }
}