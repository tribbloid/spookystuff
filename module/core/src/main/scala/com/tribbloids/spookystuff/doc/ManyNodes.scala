package com.tribbloids.spookystuff.doc

/**
  * Created by peng on 18/07/15.
  */
object ManyNodes {}

trait ManyNodes[+T <: Node] extends NodeContainer[T] {

  def nodeSeq: Seq[T]

  override def findAll(selector: DocSelector): Seq[Node] = nodeSeq.flatMap(_.findAll(selector))

  override def findAllWithSiblings(selector: DocSelector, range: Range): Seq[Siblings[Node]] =
    nodeSeq.flatMap(_.findAllWithSiblings(selector, range))

  override def children(selector: DocSelector): Seq[Node] =
    nodeSeq.flatMap(_.children(selector))

  def codes: Seq[String] = nodeSeq.flatMap(_.code)

  def formattedCodes: Seq[String] = nodeSeq.flatMap(_.formattedCode)

  def allAttrs: Seq[Map[String, String]] = nodeSeq.flatMap(_.allAttr)

  def attrs(attr: String, noEmpty: Boolean = true): Seq[String] = nodeSeq.flatMap(_.attr(attr, noEmpty))

  def hrefs: Seq[String] = nodeSeq.flatMap(_.href)

  def srcs: Seq[String] = nodeSeq.flatMap(_.src)

  def texts: Seq[String] = nodeSeq.flatMap(_.text)

  def ownTexts: Seq[String] = nodeSeq.flatMap(_.ownText)

  def boilerPipes: Seq[String] = nodeSeq.flatMap(_.boilerPipe)

  // TODO: the following headOption should become mkString
//  override def text: Option[String] = texts.headOption
//
//  override def code: Option[String] = codes.headOption
//
//  override def formattedCode: Option[String] = formattedCodes.headOption
//
//
//  override def childrenWithSiblings(selector: DocSelector, range: Range): Seq[Siblings[Part]] =
//    unbox.flatMap(_.childrenWithSiblings(selector, range))
//
//  override def ownText: Option[String] = ownTexts.headOption
//
//  override def boilerPipe: Option[String] = boilerPipes.headOption
//
//  override def allAttr: Option[Map[String, String]] = allAttrs.headOption
//
//  override def attr(attr: String, noEmpty: Boolean): Option[String] = attrs(attr, noEmpty).headOption
//
//  override def href: Option[String] = hrefs.headOption
//
//  override def src: Option[String] = srcs.headOption
//
//  override def breadcrumb: Option[Seq[String]] = unbox.head.breadcrumb
}
