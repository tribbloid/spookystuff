package com.tribbloids.spookystuff.web.actions

import com.tribbloids.spookystuff.selenium.BySizzleSelector
import com.tribbloids.spookystuff.utils.IDMixin
import org.apache.spark.ml.dsl.utils.messaging.MessageRelay
import org.apache.spark.ml.dsl.utils.refl.ScalaUDT
import org.apache.spark.sql.types.SQLUserDefinedType
import org.openqa.selenium.By

import scala.language.implicitConversions

object Selector extends MessageRelay[Selector] {

  final val SCHEMA = "By.sizzleCssSelector"

  def bySizzle(v: String) = new BySizzleSelector(v)

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
      bySizzle(_)
    )
  }
  final val factory_patterns = factories.map { fn =>
    fn -> fn("(.*)").toString.r
  }

  implicit def fromString(v: String): Selector = {

    val withPrefix = "By." + v
    for (tuple <- factory_patterns) {
      val pattern = tuple._2
      withPrefix match {
        case pattern(str) =>
          return Selector(tuple._1, str)
        case _ =>
      }
    }

    Selector(bySizzle, v)
  }

  override type M = String

  override def toMessage_>>(v: Selector): String = v.toString
}

class SelectorUDT extends ScalaUDT[Selector]

@SQLUserDefinedType(udt = classOf[SelectorUDT])
case class Selector(factory: String => By, pattern: String) extends IDMixin {

  @transient lazy val by: By = factory(pattern)

  override def toString: String = by.toString

  override lazy val _id: By = by
}
