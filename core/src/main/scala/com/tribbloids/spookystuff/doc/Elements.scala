package com.tribbloids.spookystuff.doc

/**
  * Created by peng on 18/07/15.
  */
object Elements {

  object empty extends Elements[Nothing](Nil)
}

case class Elements[+T <: Unstructured](override val originalSeq: List[T]) extends Unstructured with DelegateSeq[T] {

  def uris: Seq[String] = originalSeq.map(_.uri)

  def codes: Seq[String] = originalSeq.flatMap(_.code)

  def formattedCodes: Seq[String] = originalSeq.flatMap(_.formattedCode)

  def allAttrs: Seq[Map[String, String]] = originalSeq.flatMap(_.allAttr)

  def attrs(attr: String, noEmpty: Boolean = true): Seq[String] = originalSeq.flatMap(_.attr(attr, noEmpty))

  def hrefs: List[String] = originalSeq.flatMap(_.href)

  def srcs: List[String] = originalSeq.flatMap(_.src)

  def texts: Seq[String] = originalSeq.flatMap(_.text)

  def ownTexts: Seq[String] = originalSeq.flatMap(_.ownText)

  def boilerPipes: Seq[String] = originalSeq.flatMap(_.boilerPipe)

  override def uri: String = uris.headOption.orNull

  override def text: Option[String] = texts.headOption

  override def code: Option[String] = codes.headOption

  override def formattedCode: Option[String] = formattedCodes.headOption

  override def findAll(selector: String) = new Elements(originalSeq.flatMap(_.findAll(selector)))

  override def findAllWithSiblings(selector: String, range: Range) =
    new Elements(originalSeq.flatMap(_.findAllWithSiblings(selector, range)))

  override def children(selector: CSSQuery) = new Elements(originalSeq.flatMap(_.children(selector)))

  override def childrenWithSiblings(selector: CSSQuery, range: Range) =
    new Elements(originalSeq.flatMap(_.childrenWithSiblings(selector, range)))

  override def ownText: Option[String] = ownTexts.headOption

  override def boilerPipe: Option[String] = boilerPipes.headOption

  override def allAttr: Option[Map[String, String]] = allAttrs.headOption

  override def attr(attr: String, noEmpty: Boolean): Option[String] = attrs(attr, noEmpty).headOption

  override def href: Option[String] = hrefs.headOption

  override def src: Option[String] = srcs.headOption

  override def breadcrumb: Option[Seq[String]] = originalSeq.head.breadcrumb
}
