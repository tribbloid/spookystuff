package com.tribbloids.spookystuff.doc

import org.json4s.jackson.JsonMethods
import org.json4s.{JValue, JArray, JField}

/**
 * Created by peng on 11/30/14.
 */
object JsonElement {

  def apply(jsonStr: String, tag: String, uri: String): Unstructured = {
    val parsed: JValue = JsonMethods.parse(jsonStr)
    parsed match {
      case array: JArray =>
        val res = array.arr.map {
          field =>
            new JsonElement(tag -> field, uri)
        }
        new Siblings(res)
      case _ =>
        new JsonElement(tag -> parsed, uri)
    }
  }

  def apply(content: Array[Byte], charSet: String, uri: String): Unstructured = apply(new String(content, charSet), null, uri)
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

  override def findAll(selector: Selector): Elements[JsonElement] = {

    val selected = field._2 \\ selector

    jValueToElements(selector, selected)
  }

  //TODO: how to implement?
  override def findAllWithSiblings(selector: Selector, range: Range) = {
    val found = this.findAll(selector).self
    new Elements(found.map(unstructured => new Siblings(List(unstructured))))
  }

  private def jValueToElements(defaultFieldName: String, selected: JValue): Elements[JsonElement] = {
    selected match {
      case obj: JObject =>

        if (obj.obj.map(_._1).distinct.size <= 1) { //if the JObject contains many fields with identical names they are combined from many different places
        val jsonElements = obj.obj.map {
            field =>
              new JsonElement(field, this.uri)
          }
          new Elements(jsonElements)
        }
        else { //otherwise its a single object from the beginning
          new Elements(
            List(new JsonElement(defaultFieldName -> selected, this.uri))
          )
        }

      case array: JArray =>
        val res = array.arr.map {
          field =>
            new JsonElement(defaultFieldName -> field, this.uri)
        }
        new Siblings(res)
      case JNothing => new Elements(Nil)
      case JNull => new Elements(Nil)
      case _ =>
        new Elements(
          List(new JsonElement(defaultFieldName -> selected, this.uri))
        )
    }
  }

  override def children(selector: Selector): Elements[Unstructured] = {
    val selected = field._2 \ selector

    jValueToElements(selector, selected)
  }

  override def childrenWithSiblings(selector: Selector, range: Range): Elements[Siblings[Unstructured]] = {
    val found = this.children(selector).self
    new Elements(found.map(unstructured => new Siblings(List(unstructured))))
  }

  override def code: Option[String] = Some(JsonMethods.compact(field._2))

  override def formattedCode: Option[String] = Some(JsonMethods.pretty(field._2))

  override def allAttr: Option[Map[String, String]] = {
    val filtered = field._2.filterField{
      field =>
        field._1.startsWith("@")
    }
    val result = Map(filtered.map(v => v._1.stripPrefix("@") -> JsonMethods.compact(v._2)): _*)
    Some(result)
  }

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

  override def href: Option[String] = ownText

  override def src: Option[String] = ownText

  override def text: Option[String] = Some(field._2.values.toString)

  override def ownText: Option[String] = field._2 match {
    case obj: JObject => None
    case array: JArray => None
    case _ => Some(field._2.values.toString)
  }

  override def boilerPipe: Option[String] = None //TODO: unsupported, does it make sense

  override def toString: String = code.get

  override def breadcrumb: Option[Seq[String]] = ???
}