package com.tribbloids.spookystuff.doc

/**
  * Created by peng on 03/09/16.
  */
case class PlainElement(
                         v: String,
                         override val uri: String
                       ) extends Unstructured{

  override def findAll(selector: Selector): Elements[Unstructured] = Elements.empty

  override def findAllWithSiblings(selector: Selector, range: Range): Elements[Siblings[Unstructured]] = Elements.empty

  override def children(selector: Selector): Elements[Unstructured] = Elements.empty

  override def childrenWithSiblings(selector: Selector, range: Range): Elements[Siblings[Unstructured]] = Elements.empty

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
