package com.tribbloids.spookystuff.doc

import org.json4s.jackson.JsonMethods
import org.json4s.{JArray, JField, JValue}

import org.json4s.*

object JsonElement {

  def apply(jsonStr: String, tag: String): Unstructured = {
    val parsed: JValue =
      if (jsonStr.trim.isEmpty)
        JNull
      else {
        JsonMethods.parse(jsonStr)
      }
    parsed match {
      case array: JArray =>
        val res = array.arr.map { field =>
          new JsonElement(tag -> field)
        }
        new Siblings(res)
      case _ =>
        new JsonElement(tag -> parsed)
    }
  }
}

case class JsonElement private (
    val field: JField
) extends Unstructured {

  override def findAll(selector: DocSelector): Elements[JsonElement] = {

    val selected = field._2 \\ selector.toString

    jValueToElements(selector.toString, selected)
  }

  // TODO: how to implement?
  override def findAllWithSiblings(selector: DocSelector, range: Range): Elements[Siblings[JsonElement]] = {
    val found = this.findAll(selector).unbox
    Elements(found.map(unstructured => new Siblings(List(unstructured))))
  }

  private def jValueToElements(defaultFieldName: String, selected: JValue): Elements[JsonElement] = {
    selected match {
      case obj: JObject =>
        if (obj.obj.map(_._1).distinct.size <= 1) { // if the JObject contains many fields with identical names they are combined from many different places
          val jsonElements = obj.obj.map { field =>
            new JsonElement(field)
          }
          Elements(jsonElements)
        } else { // otherwise its a single object from the beginning
          Elements(
            List(new JsonElement(defaultFieldName -> selected))
          )
        }

      case array: JArray =>
        val res = array.arr.map { field =>
          new JsonElement(defaultFieldName -> field)
        }
        new Siblings(res)
      case JNothing => Elements(Nil)
      case JNull    => Elements(Nil)
      case _ =>
        Elements(
          List(new JsonElement(defaultFieldName -> selected))
        )
    }
  }

  override def children(selector: DocSelector): Elements[Unstructured] = {
    val selected = field._2 \ selector.toString

    jValueToElements(selector.toString, selected)
  }

  override def childrenWithSiblings(selector: DocSelector, range: Range): Elements[Siblings[Unstructured]] = {
    val found = this.children(selector).unbox
    Elements(found.map(unstructured => new Siblings(List(unstructured))))
  }

  override def code: Option[String] = Some(JsonMethods.compact(field._2))

  override def formattedCode: Option[String] = Some(JsonMethods.pretty(field._2))

  override def allAttr: Option[Map[String, String]] = {
    val filtered = field._2.filterField { field =>
      field._1.startsWith("@")
    }
    val result = Map(filtered.map(v => v._1.stripPrefix("@") -> JsonMethods.compact(v._2))*)
    Some(result)
  }

  override def attr(attr: String, noEmpty: Boolean = true): Option[String] = {

    val foundOption = field._2.findField { field =>
      field._1 == "@" + attr
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
    case _: JObject => None
    case _: JArray  => None
    case _          => Some(field._2.values.toString)
  }

  override def boilerPipe: Option[String] = None // TODO: unsupported, does it make sense

  override def toString: String = code.get

  override def breadcrumb: Option[Seq[String]] = ???
}
