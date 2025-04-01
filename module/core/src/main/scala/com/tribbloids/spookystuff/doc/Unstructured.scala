package com.tribbloids.spookystuff.doc

import org.apache.spark.sql.types.SQLUserDefinedType

object Unstructured {

  object Unrecognisable extends Unstructured {
//    override def uri: String = ""
    override def findAll(selector: DocSelector): Elements[Unstructured] = Elements.empty
    override def findAllWithSiblings(selector: DocSelector, range: Range): Elements[Siblings[Unstructured]] =
      Elements.empty
    override def children(selector: DocSelector): Elements[Unstructured] = Elements.empty
    override def childrenWithSiblings(selector: DocSelector, range: Range): Elements[Siblings[Unstructured]] =
      Elements.empty
    override def code: Option[String] = None
    override def formattedCode: Option[String] = None
    override def text: Option[String] = None
    override def ownText: Option[String] = None
    override def boilerPipe: Option[String] = None
    override def breadcrumb: Option[Seq[String]] = None
    override def allAttr: Option[Map[String, String]] = None
    override def attr(attr: String, noEmpty: Boolean): Option[String] = None
    override def href: Option[String] = None
    override def src: Option[String] = None
  }
}

@SQLUserDefinedType(udt = classOf[UnstructuredUDT])
trait Unstructured extends Serializable {
  // TODO: name should be "Selection"

//  def uri: String

  def findAll(selector: DocSelector): Elements[Unstructured]

  final def select(selector: DocSelector): Elements[Unstructured] = findAll(selector)

  final def \(selector: DocSelector): Elements[Unstructured] = findAll(selector)

  final def findFirst(selector: DocSelector): Option[Unstructured] =
    findAll(selector).headOption

  def findAllWithSiblings(
      selector: DocSelector,
      range: Range
  ): Elements[Siblings[Unstructured]]

  final def findFirstWithSiblings(selector: DocSelector, range: Range): Option[Siblings[Unstructured]] =
    findAllWithSiblings(selector, range).headOption

  def children(selector: DocSelector): Elements[Unstructured]

  final def child(selector: DocSelector): Option[Unstructured] =
    children(selector).headOption

  def childrenWithSiblings(
      selector: DocSelector,
      range: Range
  ): Elements[Siblings[Unstructured]]

  final def childWithSiblings(selector: DocSelector, range: Range): Option[Siblings[Unstructured]] =
    findAllWithSiblings(selector, range).headOption

  def code: Option[String]

  def formattedCode: Option[String]

  def text: Option[String]

  def ownText: Option[String]

  def boilerPipe: Option[String]

  def breadcrumb: Option[Seq[String]]

  def allAttr: Option[Map[String, String]]

  def attr(attr: String, noEmpty: Boolean = true): Option[String]

  def href: Option[String]

  def src: Option[String]
}
