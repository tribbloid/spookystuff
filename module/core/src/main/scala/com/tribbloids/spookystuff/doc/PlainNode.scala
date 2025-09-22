package com.tribbloids.spookystuff.doc

/**
  * Created by peng on 03/09/16.
  */
case class PlainNode(
    v: String
//    val uri: String
) extends Node {

  override def findAll(selector: DocSelector): Seq[Node] = Seq.empty

  override def findAllWithSiblings(selector: DocSelector, range: Range): Seq[Siblings[Node]] = Seq.empty

  override def children(selector: DocSelector): Seq[Node] = Seq.empty

  override def childrenWithSiblings(selector: DocSelector, range: Range): Seq[Siblings[Node]] = Seq.empty

  override def code: Option[String] = Some(v)

  override def formattedCode: Option[String] = Some(v)

  override def text: Option[String] = Some(v)

  override def ownText: Option[String] = Some(v)

  override def boilerPipe: Option[String] = Some(v)

  override def breadcrumb: Option[Seq[String]] = Some(Nil)

  override def allAttr: Option[Map[String, String]] = None

  override def attr(attr: String, noEmpty: Boolean): Option[String] = None

  override def href: Option[String] = None

  override def src: Option[String] = None
}
