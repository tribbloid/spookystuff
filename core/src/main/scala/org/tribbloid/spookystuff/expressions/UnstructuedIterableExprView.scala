package org.tribbloid.spookystuff.expressions

import java.util.Date

import org.tribbloid.spookystuff.dsl
import org.tribbloid.spookystuff.pages.{Page, Unstructured}

import scala.collection.immutable.ListSet

/**
 * Created by peng on 11/29/14.
 */
final class UnstructuedIterableExprView(self: Expression[Iterable[Unstructured]]) {

  import dsl._

  def uris: Expression[Iterable[String]] = self.andMap(_.uris, "uris")

  def allChildren(selector: String): Expression[Iterable[Unstructured]] = self.andMap(_.allChildren(selector), s"allChildren($selector)")

  def texts: Expression[Iterable[String]] = self.andMap(_.texts, "texts")

  def ownTexts: Expression[Iterable[String]] = self.andMap(_.ownTexts, "ownTexts")

  def attrs(attrKey: String, noEmpty: Boolean = true): Expression[Iterable[String]] = self.andMap(_.attrs(attrKey, noEmpty), s"attrs($attrKey,$noEmpty)")

  def hrefs = attrs("abs:href", noEmpty = true)

  def srcs = attrs("abs:src", noEmpty = true)

  //  def boilerPipe
}

class PageSeqExprView(self: Expression[Iterable[Page]]) {

  import dsl._

  def timestamps: Expression[Iterable[Date]] = self.andMap(_.map(_.timestamp), "timestamps")

  def saveds: Expression[Iterable[ListSet[String]]] = self.andMap(_.map(_.saved), "saveds")
}