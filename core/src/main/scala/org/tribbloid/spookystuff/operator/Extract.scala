package org.tribbloid.spookystuff.operator

import java.util.UUID

import org.tribbloid.spookystuff.Utils
import org.tribbloid.spookystuff.entity.Page

/**
 * Created by peng on 8/29/14.
 */
abstract class Extract[T] extends (Page => T) with Serializable {

  //this is used to delay for element to exist
  val selectors: Seq[String] = Seq()
}

//case object ExtractTrue extends Extract[Boolean] {
//
//  override def apply(page: Page): Boolean = {
//
//    true
//  }
//}

//case class TimestampPath(lookup: Lookup[_]) extends Extract[String] {
//
//  override def apply(page: Page): String = {
//
//    Utils.urlConcat(lookup(page.uid).toString, page.timestamp.getTime.toString)
//  }
//}

case class UUIDPath(lookup: Lookup[_]) extends Extract[String] {

  override def apply(page: Page): String = {

    Utils.urlConcat(lookup(page.uid).toString, UUID.randomUUID().toString)
  }
}

//case class ExtractIfElementExist(override val selector: String) extends Extract[Boolean] {
//
//  override def apply(page: Page): Boolean = {
//
//    page.elementExist(selector)
//  }
//}

abstract class FromElement[T](selector: String) extends Extract[T] {

  override val selectors = Seq(selector)
}

case class NumElements(selector: String) extends FromElement[Int](selector) {

  override def apply(page: Page): Int = page.numElements(selector)
}

case class ElementExist(selector: String) extends FromElement[Boolean](selector) {

  override def apply(page: Page): Boolean = page.elementExist(selector)
}

case class AttrExist(
                      selector: String,
                      attr: String
                      ) extends FromElement[Boolean](selector) {

  override def apply(page: Page): Boolean = page.attrExist(selector, attr)
}