package org.tribbloid.spookystuff.pages

/**
 * Created by peng on 11/27/14.
 */
trait Unstructured extends Serializable {

  def uri: String

  def children(selector: String): Elements[Unstructured]

  final def child(selector: String): Option[Unstructured] = children(selector).headOption

  def childrenWithSiblings(
                        selector: String,
                        range: Range
                        ): Elements[Siblings[Unstructured]]

  final def childExpanded(selector: String, range: Range): Option[Siblings[Unstructured]] = childrenWithSiblings(selector, range).headOption

  def code: Option[String]

  def attr(attr: String, noEmpty: Boolean = true): Option[String]

  final def href = attr("abs:href") //TODO: if this is identical to the uri itself, it should be considered an inline/invalid link and have None output

  final def src = attr("abs:src")

  def text: Option[String]

  def ownText: Option[String]

  def boilerPipe: Option[String]
}

class Elements[+T <: Unstructured](self: Seq[T]) extends Unstructured with Seq[T] {

  def uris: Seq[String] = self.map(_.uri)

  def codes: Seq[String] = self.flatMap(_.code)

  def attrs(attr: String, noEmpty: Boolean = true): Seq[String] = self.flatMap(_.attr(attr, noEmpty))

  def hrefs(abs: Boolean) = attrs("abs:href")

  def srcs = attrs("abs:src")

  def texts: Seq[String] = self.flatMap(_.text)

  def ownTexts: Seq[String] = self.flatMap(_.ownText)

  def boilerPipes: Seq[String] = self.flatMap(_.boilerPipe)

  override def uri: String = uris.headOption.orNull

  override def text: Option[String] = texts.headOption

  override def code: Option[String] = codes.headOption

  override def children(selector: String) = new Elements(self.flatMap(_.children(selector)))

  override def childrenWithSiblings(start: String, range: Range) = new Elements(self.flatMap(_.childrenWithSiblings(start, range)))

  override def ownText: Option[String] = ownTexts.headOption

  override def boilerPipe: Option[String] = boilerPipes.headOption

  override def attr(attr: String, noEmpty: Boolean): Option[String] = attrs(attr, noEmpty).headOption

  override def iterator: Iterator[T] = self.iterator

  override def length: Int = self.length

  override def apply(idx: Int): T = self.apply(idx)
}

class Siblings[+T <: Unstructured](self: Seq[T]) extends Elements[T](self) {

  override def text = if (texts.isEmpty) None
  else Some(texts.filter(_.nonEmpty).mkString(" "))

  override def code = if (codes.isEmpty) None
  else Some(codes.filter(_.nonEmpty).mkString(" "))

  override def ownText = if (ownTexts.isEmpty) None
  else Some(ownTexts.filter(_.nonEmpty).mkString(" "))

  override def boilerPipe = if (boilerPipes.isEmpty) None
  else Some(boilerPipes.filter(_.nonEmpty).mkString(" "))
}