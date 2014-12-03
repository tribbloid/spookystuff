package org.tribbloid.spookystuff.expressions

import java.util.Date

import org.tribbloid.spookystuff.dsl
import org.tribbloid.spookystuff.pages.{Page, Unstructured}

/**
 * Created by peng on 11/29/14.
 */
class UnstructuedExprView(self: Expr[Unstructured]) {

  import dsl._

  def uri: Expr[String] = self.andMap(_.uri, "uri")

  def children(selector: String): Expr[Seq[Unstructured]] = self.andMap(_.children(selector), s"children($selector)")

  def text: Expr[String] = self.andFlatMap(_.text, "text")

  def ownText: Expr[String] = self.andFlatMap(_.ownText, "ownText")

  def attr(attrKey: String, noEmpty: Boolean = true): Expr[String] = self.andFlatMap(_.attr(attrKey, noEmpty), s"attr($attrKey,$noEmpty)")

  def href = attr("abs:href", noEmpty = true)

  def src = attr("abs:src", noEmpty = true)

  //  def boilerPiple
}

class PageExprView(self: Expr[Page]) {

  import dsl._

  def timestamp: Expr[Date] = self.andMap(_.timestamp, "timestamp")

  def saved = self.andMap(_.saved, "saved")
}