package com.tribbloids.spookystuff.web.actions

import com.tribbloids.spookystuff.relay.{IR, Relay, TreeIR}
import com.tribbloids.spookystuff.selenium.BySizzleSelector
import org.apache.spark.sql.types.SQLUserDefinedType
import org.openqa.selenium.By

import scala.language.implicitConversions
import scala.util.matching.Regex

object Selector extends Relay.ToMsg[Selector] {

  final val SCHEMA = "By.sizzleCssSelector"

  def bySizzle(v: String): BySizzleSelector = new BySizzleSelector(v)

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
  final val factory_patterns: Seq[(String => By, Regex)] = factories.map { fn =>
    fn -> fn("(.*)").toString.r
  }

  implicit def fromString(v: String): Selector = {

    val withPrefix = "By." + v
    for (tuple <- factory_patterns) {
      val pattern = tuple._2
      withPrefix match {
        case pattern(str) =>
          return Selector(tuple._1(str))
        case _ =>
      }
    }

    Selector(bySizzle(v))
  }

  override type Msg = String

  override def toMessage_>>(v: Selector) = TreeIR.leaf(v.toString)

  override def toProto_<<(v: IR.Aux[String]): Selector = ???
}

@SQLUserDefinedType(udt = classOf[SelectorUDT])
case class Selector(by: By) {

  override def toString: String = by.toString

}
