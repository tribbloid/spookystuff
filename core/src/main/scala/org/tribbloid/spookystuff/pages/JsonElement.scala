package org.tribbloid.spookystuff.pages

import java.nio.charset.Charset

import org.json4s.{JArray, JField}
import org.json4s.jackson.JsonMethods

/**
 * Created by peng on 11/30/14.
 */
object JsonElement {

  def apply(jsonStr: String, tag: String, uri: String): Unstructured = {
    val parsed = JsonMethods.parse(jsonStr)
    parsed match {
      case array: JArray =>
        val res = array.arr.map{
          field =>
            new JsonElement(tag -> field, uri)
        }.toArray
        new Siblings(res)
      case _ =>
        new JsonElement(tag -> parsed, uri)
    }
  }

  def apply(content: Array[Byte], charSet: Charset, uri: String): Unstructured = apply(new String(content, charSet), null, uri)
}

class JsonElement private (
                            val field: JField,
                            override val uri: String
                            ) extends Unstructured {

  import org.json4s._



  override def equals(obj: Any): Boolean = obj match {
    case other: JsonElement =>
      (this.field == other.field) && (this.uri == other.uri)
    case _ => false
  }

  override def hashCode(): Int = (this.field, this.uri).hashCode()

  override def children(selector: Selector) = {

    val selected = field._2 \\ selector

    selected match {
      case obj: JObject =>
        val res = obj.obj.map{
          field =>
            new JsonElement(field, this.uri)
        }.toArray
        new Elements(res)
      case array: JArray =>
        val res = array.arr.map{
          field =>
            new JsonElement(selector -> field, this.uri)
        }.toArray
        new Siblings(res)
      case _ =>
        new Elements(
          Array(new JsonElement(selector -> selected, this.uri))
        )
    }
  }

  override def childrenWithSiblings(selector: Selector, range: Range) = //TODO: how to implement?
    children(selector).map[Siblings[Unstructured], Elements[Siblings[Unstructured]]](unstructured => new Siblings(Array(unstructured)))

  override def code: Option[String] = Some(JsonMethods.compact(field._2))

  override def attr(attr: String, noEmpty: Boolean = true): Option[String] = {

    val foundOption = field._2.findField{
      field =>
        field._1 == "@"+attr
    }

    val result = foundOption.map(found => JsonMethods.compact(found._2))

    result match {
      case None => None
      case Some(str) =>
        if (noEmpty && str.trim.replaceAll("\u00A0", "").isEmpty) None
        else result
    }
  }

  override def text: Option[String] = Some(field._2.values.toString)

  override def ownText: Option[String] = field._2 match {
    case obj: JObject => None
    case array: JArray => None
    case _ => Some(field._2.values.toString)
  }

  override def boilerPipe: Option[String] = None //unsupported

  override def toString: String = code.get
}