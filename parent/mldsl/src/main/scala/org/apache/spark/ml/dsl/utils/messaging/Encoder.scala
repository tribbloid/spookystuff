package org.apache.spark.ml.dsl.utils.messaging

import org.apache.spark.ml.dsl.utils._
import org.apache.spark.ml.dsl.utils.messaging.xml.{XMLFormats, Xml}
import org.json4s.JsonAST.JObject
import org.json4s.jackson.JsonMethods._
import org.json4s.{Extraction, Formats, JValue}

import java.io.File
import scala.xml.NodeSeq

case class Encoder[M](
    message: M,
    formats: Formats = XMLFormats.defaultFormats,
    rootTagOverride: Option[String] = None
) extends Serializable
    with RootTagged {

  override lazy val rootTag: String = rootTagOverride.getOrElse(
    Relay.RootTagOf(message).default
  )

  // TODO: move into case class WFormats(.) and enable lazy val
  def toJValue(
      implicit
      formats: Formats = formats
  ): JValue = Extraction.decompose(message)
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
    Xml.toXml(JObject(rootTag -> toJValue))
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

  def cast[T](formats: Formats = formats)(reader: Relay[T]#Decoder): T = {
    reader.fromJValue(toJValue(formats))
  }

  // TODO: delegate to TreeIR
  def getMemberStr(
      start: String = "(",
      sep: String = ",",
      end: String = ")",
      indentFn: Int => String = _ => "",
      recursion: Int = 0
  ): String = {

    val indentStr = indentFn(recursion)

    def listRecursion(elems: Traversable[Any]): List[String] = {
      elems.toList
        .map { vv =>
          val str = Encoder(vv).getMemberStr(start, sep, end, indentFn, recursion + 1)
          DSLUtils.indent(str, indentStr)
        }
    }

    def mapRecursion[T](map: Map[T, Any]): Seq[String] = {
      map.toSeq
        .map {
          case (kk, vv) =>
            val vvStr = Encoder(vv).getMemberStr(start, sep, end, indentFn, recursion + 1)
            DSLUtils.indent(s"$kk=$vvStr", indentStr)
        }
    }

    def product2Str(v: Product): String = {
      val elems = v.productIterator.toList

      val vIsSingleton = {

        val className = v.getClass.getName
        className.endsWith("$")
      }

      val concat = if (elems.isEmpty || vIsSingleton) {
        rootTag
      } else {
        val strs: List[String] = listRecursion(elems)

        strs.mkString(rootTag + start, sep, end)
      }
      concat
    }

    message match {

      case is: TreeIR.ProductMap =>
        product2Str(is)

      case is: Map[_, _] =>
        val strs = mapRecursion(is)
        val concat =
          if (strs.nonEmpty)
            strs.mkString(rootTag + start, sep, end)
          else
            rootTag + start + end
        concat

      case is: Traversable[_] =>
        val strs = listRecursion(is)
        val concat =
          if (strs.nonEmpty)
            strs.mkString(rootTag + start, sep, end)
          else
            rootTag + start + end
        concat

      case v: Product =>
        product2Str(v)

      case _ =>
        "" + message // TODO: should we allow this fallback?
    }
  }

  lazy val memberStr: String = this.getMemberStr()
  lazy val memberStr_\\\ : String = this.getMemberStr(File.separator, File.separator, File.separator)
  lazy val memberStr_/:/ : String = this.getMemberStr("/", "/", "/")
  lazy val memberStrPretty: String = this.getMemberStr(
    "(\n",
    ",\n",
    "\n)",
    { _ =>
      "  "
    }
  )
}

object Encoder {}
