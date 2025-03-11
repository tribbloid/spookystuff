package com.tribbloids.spookystuff.doc

import ai.acyclic.prover.commons.Delegating

/**
  * Created by peng on 18/07/15.
  */
object Elements {

  case class ^[+T <: Unstructured](override val unbox: Seq[T]) extends Elements[T] {}

  def apply[T <: Unstructured](unbox: Seq[T]): ^[T] = ^(unbox)

  object empty extends ^[Nothing](Nil)

}
trait Elements[+T <: Unstructured] extends Unstructured with Delegating[Seq[T]] {

  def uris: Seq[String] = unbox.map(_.uri)

  @transient override lazy val uri: String = {
    val distinct = uris.distinct
    require(distinct.size == 1, "No element or elements have different URIs: " + distinct.mkString(", "))
    distinct.head
  }

  def codes: Seq[String] = unbox.flatMap(_.code)

  def formattedCodes: Seq[String] = unbox.flatMap(_.formattedCode)

  def allAttrs: Seq[Map[String, String]] = unbox.flatMap(_.allAttr)

  def attrs(attr: String, noEmpty: Boolean = true): Seq[String] = unbox.flatMap(_.attr(attr, noEmpty))

  def hrefs: Seq[String] = unbox.flatMap(_.href)

  def srcs: Seq[String] = unbox.flatMap(_.src)

  def texts: Seq[String] = unbox.flatMap(_.text)

  def ownTexts: Seq[String] = unbox.flatMap(_.ownText)

  def boilerPipes: Seq[String] = unbox.flatMap(_.boilerPipe)

  // TODO: the following headOption should become mkString
  override def text: Option[String] = texts.headOption

  override def code: Option[String] = codes.headOption

  override def formattedCode: Option[String] = formattedCodes.headOption

  override def find(selector: DocQuery): Elements[Unstructured] = Elements(unbox.flatMap(_.find(selector)))

  override def findAllWithSiblings(selector: DocQuery, range: Range): Elements[Siblings[Unstructured]] =
    Elements(unbox.flatMap(_.findAllWithSiblings(selector, range)))

  override def children(selector: DocQuery): Elements[Unstructured] = Elements(
    unbox.flatMap(_.children(selector))
  )

  override def childrenWithSiblings(selector: DocQuery, range: Range): Elements[Siblings[Unstructured]] =
    Elements(unbox.flatMap(_.childrenWithSiblings(selector, range)))

  override def ownText: Option[String] = ownTexts.headOption

  override def boilerPipe: Option[String] = boilerPipes.headOption

  override def allAttr: Option[Map[String, String]] = allAttrs.headOption

  override def attr(attr: String, noEmpty: Boolean): Option[String] = attrs(attr, noEmpty).headOption

  override def href: Option[String] = hrefs.headOption

  override def src: Option[String] = srcs.headOption

  override def breadcrumb: Option[Seq[String]] = unbox.head.breadcrumb
}
