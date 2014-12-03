package org.tribbloid.spookystuff.expressions

import org.tribbloid.spookystuff.dsl
import org.tribbloid.spookystuff.pages.Unstructured

/**
 * Created by peng on 11/29/14.
 */
class UnstructuedSeqExprView(self: Expr[Seq[Unstructured]]) {

  import dsl._

  def allChildren(selector: String): Expr[Seq[Unstructured]] = self.map(_.allChildren(selector), s"select($selector)")

  def texts: Expr[Seq[String]] = self.map(_.texts, "text")

  def ownTexts: Expr[Seq[String]] = self.map(_.ownTexts, "ownText")

  def attrs(attrKey: String, noEmpty: Boolean = true): Expr[Seq[String]] = self.map(_.attrs(attrKey, noEmpty), s"attr($attrKey,$noEmpty)")

  def hrefs = attrs("abs:href", noEmpty = true)

  def srcs = attrs("abs:src", noEmpty = true)

  //  def boilerPiple
}
