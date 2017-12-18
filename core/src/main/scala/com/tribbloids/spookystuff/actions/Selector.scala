package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.selenium.BySizzleCssSelector
import org.apache.spark.ml.dsl.utils.refl.ScalaUDT
import org.apache.spark.ml.dsl.utils.messaging.MessageRelay
import org.apache.spark.sql.types.SQLUserDefinedType
import org.openqa.selenium.By

import scala.language.implicitConversions

object Selector extends MessageRelay[Selector]{

  final val SCHEMA = "By.sizzleCssSelector"

  final val factories: Seq[String => By] = {
    Seq(
      By.id(_),
      By.name(_),
      By.cssSelector(_),
      By.linkText(_),
      By.className(_),
      By.tagName(_),
      By.xpath(_),
      By.partialLinkText(_),
      v => new BySizzleCssSelector(v)
    )
  }
  final val factory_patterns = factories.map {
    fn =>
      fn -> fn("(.*)").toString.r
  }

  implicit def fromString(v: String): Selector = {

    val withPrefix = "By." + v
    for (tuple <- factory_patterns) {
      val pattern = tuple._2
      withPrefix match {
        case pattern(selector) =>
          return Selector(tuple._1(selector))
        case _ =>
      }
    }

    Selector(new BySizzleCssSelector(v))
  }

  override type M = String

  override def toMessage_>>(v: Selector) = v.toString
}

@SQLUserDefinedType(udt = classOf[SelectorUDT])
case class Selector(by: By) {

  override def toString: String = by.toString
}

class SelectorUDT extends ScalaUDT[Selector]