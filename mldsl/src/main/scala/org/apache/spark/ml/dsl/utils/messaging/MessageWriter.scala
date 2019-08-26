package org.apache.spark.ml.dsl.utils.messaging

import java.io.File

import org.apache.spark.ml.dsl.utils._
import org.apache.spark.ml.dsl.utils.refl.ScalaType
import org.json4s.JsonAST.JObject
import org.json4s.jackson.JsonMethods.{pretty, _}
import org.json4s.{Extraction, Formats, JValue}

import scala.language.implicitConversions
import scala.xml.NodeSeq

class MessageWriter[M](
    val message: M,
    val formats: Formats = Xml.defaultFormats,
    rootTagOverride: Option[String] = None
) extends Serializable {

  def rootTag: String = rootTagOverride.getOrElse(
    Codec.getRootTag(message)
  )

  //TODO: move into case class WFormats(.) and enable lazy val
  def toJValue(implicit formats: Formats = formats): JValue = Extraction.decompose(message)
  def compactJSON(implicit formats: Formats = formats): String = compact(render(toJValue))
  def prettyJSON(implicit formats: Formats = formats): String = pretty(render(toJValue))
  def toJSON(pretty: Boolean = true)(implicit formats: Formats = formats): String = {
    if (pretty) prettyJSON(formats)
    else compactJSON(formats)
  }

  def toXMLNode(implicit formats: Formats = formats): NodeSeq =
    Xml.toXml(JObject(rootTag -> toJValue))
  def compactXML(implicit formats: Formats = formats): String = toXMLNode.toString().replaceAllLiterally("\n", "")
  def prettyXML(implicit formats: Formats = formats): String = Xml.defaultXMLPrinter.formatNodes(toXMLNode)
  def toXMLStr(pretty: Boolean = true)(implicit formats: Formats = formats): String = {
    if (pretty) prettyXML
    else compactXML
  }

  def cast[T: Codec](formats: Formats = formats) = {
    MessageReader._fromJValue[T](toJValue(formats))
  }

  //TODO: delegate to Nested
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
          MessageWriter(vv).getMemberStr(start, sep, end, indentFn, recursion + 1)
        }
        .map { str =>
          FlowUtils.indent(str, indentStr)
        }
    }

    def mapRecursion[T](map: Map[T, Any]): Map[T, String] = {
      map
        .mapValues { vv =>
          MessageWriter(vv).getMemberStr(start, sep, end, indentFn, recursion + 1)
        }
        .mapValues { str =>
          FlowUtils.indent(str, indentStr)
        }
    }

    def product2Str(v: Product): String = {
      val elems = v.productIterator.toList
      val runtimeType = ScalaType.getRuntimeType(v)

      val concat = if (elems.isEmpty || runtimeType.asClass.getCanonicalName.endsWith("$")) {
        rootTag
      } else {
        val strs: List[String] = listRecursion(elems)

        strs.mkString(rootTag + start, sep, end)
      }
      concat
    }

    message match {
      case v: GenericProduct[_] =>
        product2Str(v)

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

  lazy val memberStr = this.getMemberStr()
  lazy val memberStr_\\\ = this.getMemberStr(File.separator, File.separator, File.separator)
  lazy val memberStr_/:/ = this.getMemberStr("/", "/", "/")
  lazy val memberStrPretty = this.getMemberStr("(\n", ",\n", "\n)", { _ =>
    "\t"
  })
}

object MessageWriter {

  def apply[M](
      message: M,
      formats: Formats = Xml.defaultFormats,
      rootTagOverride: Option[String] = None
  ): MessageWriter[M] = new MessageWriter[M](message, formats, rootTagOverride)
}
