package org.tribbloid.spookystuff.expressions

import java.util.Date

import org.tribbloid.spookystuff.pages.{Elements, Page}
import scala.collection.immutable.ListSet

import org.tribbloid.spookystuff.dsl._

/**
 * Created by peng on 11/29/14.
 */
final class ElementsExprView(self: Expression[Elements[_]]) {

  def uris: Expression[Seq[String]] = self.andMap(_.uris, "uris")

  def texts: Expression[Seq[String]] = self.andMap(_.texts, "texts")

  def codes: Expression[Seq[String]] = self.andMap(_.codes, "text")

  def ownTexts: Expression[Seq[String]] = self.andMap(_.ownTexts, "ownTexts")

  def attrs(attrKey: String, noEmpty: Boolean = true): Expression[Seq[String]] = self.andMap(_.attrs(attrKey, noEmpty), s"attrs($attrKey,$noEmpty)")

  def hrefs = attrs("abs:href", noEmpty = true)

  def srcs = attrs("abs:src", noEmpty = true)

  def boilerPipes = self.andMap(_.boilerPipes, "text")
}

class PageSeqExprView(self: Expression[Iterable[Page]]) {

  def timestamps: Expression[Iterable[Date]] = self.andMap(_.map(_.timestamp), "timestamps")

  def saveds: Expression[Iterable[ListSet[String]]] = self.andMap(_.map(_.saved), "saveds")
}