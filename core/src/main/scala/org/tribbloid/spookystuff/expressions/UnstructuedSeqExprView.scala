package org.tribbloid.spookystuff.expressions

import org.tribbloid.spookystuff.dsl
import org.tribbloid.spookystuff.pages.Unstructured

/**
 * Created by peng on 11/29/14.
 */
class UnstructuedSeqExprView(self: Expr[Seq[Unstructured]]) {

  import dsl._

  def allChildren(selector: String): Expr[Seq[Unstructured]] = self.andMap(_.allChildren(selector), s"allChildren($selector)")

  def texts: Expr[Seq[String]] = self.andMap(_.texts, "texts")

  def ownTexts: Expr[Seq[String]] = self.andMap(_.ownTexts, "ownTexts")

  def attrs(attrKey: String, noEmpty: Boolean = true): Expr[Seq[String]] = self.andMap(_.attrs(attrKey, noEmpty), s"attrs($attrKey,$noEmpty)")

  def hrefs = attrs("abs:href", noEmpty = true)

  def srcs = attrs("abs:src", noEmpty = true)

  //  def boilerPiple
}
