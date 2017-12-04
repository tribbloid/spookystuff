package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.selenium.BySizzleCssSelector
import com.tribbloids.spookystuff.utils.refl.ScalaUDT
import org.apache.spark.ml.dsl.utils.messaging.MessageRelay
import org.apache.spark.sql.types.SQLUserDefinedType
import org.openqa.selenium.By

import scala.language.implicitConversions

object Selector extends MessageRelay[Selector]{

  final val SCHEMA = "By.sizzleCssSelector"

  implicit def fromString(v: String) = {

    val splitted = v.split(":")
    val (schema, vv) = if (splitted.size <= 1) {
      SCHEMA -> v.trim
    }
    else {
      splitted.head.trim -> splitted.slice(1, Int.MaxValue).mkString(":")
    }

    val by = schema match {
      case "By.id" => By.id(vv)
      case "By.name" => By.name(vv)
      case "By.cssSelector" => By.cssSelector(vv)
      case "By.linkText" => By.linkText(vv)
      case "By.className" => By.className(vv)
      case "By.tagName" => By.tagName(vv)
      case "By.xpath" => By.xpath(vv)
      case "By.partialLinkText" => By.partialLinkText(vv)
      case SCHEMA => new BySizzleCssSelector(v)
    }

    Selector(by)
  }

  override type M = String

  override def toM(v: Selector) = v.toString
}

@SQLUserDefinedType(udt = classOf[SelectorUDT])
case class Selector(by: By) extends Selector.API {

  override def toString: String = by.toString
}

class SelectorUDT extends ScalaUDT[Selector]