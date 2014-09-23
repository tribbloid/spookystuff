package org.tribbloid.spookystuff.operator

import org.openqa.selenium.By
import org.openqa.selenium.support.ui.{ExpectedCondition, ExpectedConditions}
import org.tribbloid.spookystuff.Utils
import org.tribbloid.spookystuff.entity.Page

/**
 * Created by peng on 8/29/14.
 */
abstract class Extract[T] extends (Page => T) with Serializable with Product {

  val selector: String = null
}

case object ExtractTrue extends Extract[Boolean] {

  override def apply(page: Page): Boolean = {

    true
  }
}

case class ExtractTimestamp(lookup: Lookup[_]) extends Extract[String] {

  override def apply(page: Page): String = {

    Utils.urlConcat(lookup(page.uid).toString, page.timestamp.getTime.toString)
  }
}

case class ExtractIfElementExist(override val selector: String) extends Extract[Boolean] {

  override def apply(page: Page): Boolean = {

    page.elementExist(selector)
  }
}
