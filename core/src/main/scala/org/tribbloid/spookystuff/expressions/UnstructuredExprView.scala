package org.tribbloid.spookystuff.expressions

import java.util.Date

import org.tribbloid.spookystuff.dsl
import org.tribbloid.spookystuff.pages.{Elements, Page, Unstructured}

import scala.collection.immutable.ListSet

/**
 * Created by peng on 11/29/14.
 */
final class UnstructuredExprView(self: Expression[Unstructured]) {

  import dsl._

  def uri: Expression[String] = self.andMap(_.uri, "uri")

  def child(selector: String): ChildExpr = new ChildExpr(selector, self)

  def children(selector: String): ChildrenExpr = new ChildrenExpr(selector, self)

  def text: Expression[String] = self.andFlatMap(_.text, "text")

  def code = self.andFlatMap(_.code, "text")

  def ownText: Expression[String] = self.andFlatMap(_.ownText, "ownText")

  def attr(attrKey: String, noEmpty: Boolean = true): Expression[String] = self.andFlatMap(_.attr(attrKey, noEmpty), s"attr($attrKey,$noEmpty)")

  def href = attr("abs:href", noEmpty = true)

  def src = attr("abs:src", noEmpty = true)

  def boilerPiple = self.andFlatMap(_.boilerPipe, "boilerPipe")
}

class PageExprView(self: Expression[Page]) {

  import dsl._

  def timestamp: Expression[Date] = self.andMap(_.timestamp, "timestamp")

  def saved: Expression[ListSet[String]] = self.andMap(_.saved, "saved")
}