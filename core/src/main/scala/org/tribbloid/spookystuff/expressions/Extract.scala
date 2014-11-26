package org.tribbloid.spookystuff.expressions

import java.util.UUID
import org.tribbloid.spookystuff.entity.Page
import org.tribbloid.spookystuff.utils.Utils

import scala.runtime.ScalaRunTime

/**
 * Created by peng on 8/29/14.
 */
trait Extract[+T] extends (Page => T) with Serializable with Product {

  //this is used to delay for element to exist
  val selectors: Seq[String] = Seq()

  override def toString() = ScalaRunTime._toString(this)
}

case class UUIDPath(encoder: TraceEncoder[_]) extends Extract[String] {

  override def apply(page: Page): String = {

    Utils.urlConcat(encoder(page.uid.backtrace).toString, UUID.randomUUID().toString)
  }
}

abstract class FromElements[T](selector: String) extends Extract[T] {

  override val selectors = Seq(selector)
}

//case class ElementExist(selector: String) extends FromElements[Boolean](selector) {
//
//  override def apply(page: Page): Boolean = page.elementExist(selector)
//}
//
//case class NumElements(selector: String) extends FromElements[Int](selector) {
//
//  override def apply(page: Page): Int = page.numElements(selector)
//}
//
//case class AttrExist(
//                      selector: String,
//                      attr: String
//                      ) extends FromElements[Boolean](selector) {
//
//  override def apply(page: Page): Boolean = page.attrExist(selector, attr)
//}

case class Attr(
                 selector: String,
                 attr: String,
                 noEmpty: Boolean
                 ) extends FromElements[Seq[String]](selector) {

  override def apply(page: Page): Seq[String] = page.attr(selector, attr, noEmpty)
}

object Href {

  def apply(
             selector: String,
             absolute: Boolean,
             noEmpty: Boolean
             ): Attr = {
    val attr = if (absolute) "abs:href"
    else "href"

    Attr(selector, attr, noEmpty)
  }
}

object Src {

  def apply(
             selector: String,
             absolute: Boolean,
             noEmpty: Boolean
             ): Attr = {
    val attr = if (absolute) "abs:src"
    else "src"

    Attr(selector, attr, noEmpty)
  }
}

case class Text(
                 selector: String,
                 own: Boolean
                 ) extends FromElements[Seq[String]](selector) {

  override def apply(page: Page): Seq[String] = page.text(selector, own)
}