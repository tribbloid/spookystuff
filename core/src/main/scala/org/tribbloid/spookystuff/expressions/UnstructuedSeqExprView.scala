package org.tribbloid.spookystuff.expressions

import java.util.Date

import org.tribbloid.spookystuff.dsl
import org.tribbloid.spookystuff.pages.{Page, Unstructured}

import scala.collection.immutable.ListSet

/**
 * Created by peng on 11/29/14.
 */
final class UnstructuedSeqExprView(self: Expression[Seq[Unstructured]]) {

  import dsl._

  def uris: Expression[Seq[String]] = self.andMap(_.uris, "uris")

  def allChildren(selector: String): Expression[Seq[Unstructured]] = self.andMap(_.allChildren(selector), s"allChildren($selector)")

  def texts: Expression[Seq[String]] = self.andMap(_.texts, "texts")

  def ownTexts: Expression[Seq[String]] = self.andMap(_.ownTexts, "ownTexts")

  def attrs(attrKey: String, noEmpty: Boolean = true): Expression[Seq[String]] = self.andMap(_.attrs(attrKey, noEmpty), s"attrs($attrKey,$noEmpty)")

  def hrefs = attrs("abs:href", noEmpty = true)

  def srcs = attrs("abs:src", noEmpty = true)

  //  def boilerPiple
}

class PageSeqExprView(self: Expression[Seq[Page]]) {

  import dsl._

  def timestamps: Expression[Seq[Date]] = self.andMap(_.map(_.timestamp), "timestamps")

  def saveds: Expression[Seq[ListSet[String]]] = self.andMap(_.map(_.saved), "saveds")
}