package com.tribbloids.spookystuff.doc

import com.tribbloids.spookystuff.utils.ScalaUDT
import org.apache.spark.sql.types.SQLUserDefinedType

class UnstructuredUDT extends ScalaUDT[Unstructured]

@SQLUserDefinedType(udt = classOf[UnstructuredUDT])
trait Unstructured extends Serializable {

  def uri: String

  def findAll(selector: Selector): Elements[Unstructured]

  final def \\(selector: Selector) = findAll(selector)

  final def findFirst(selector: Selector): Option[Unstructured] =
    findAll(selector).headOption

  def findAllWithSiblings(
                            selector: Selector,
                            range: Range
                            ): Elements[Siblings[Unstructured]]

  final def findFirstWithSiblings(selector: Selector, range: Range): Option[Siblings[Unstructured]] =
    findAllWithSiblings(selector, range).headOption

  def children(selector: Selector): Elements[Unstructured]

  final def \(selector: Selector) = findAll(selector)

  final def child(selector: Selector): Option[Unstructured] =
    children(selector).headOption

  def childrenWithSiblings(
                           selector: Selector,
                           range: Range
                           ): Elements[Siblings[Unstructured]]

  final def childWithSiblings(selector: Selector, range: Range): Option[Siblings[Unstructured]] =
    findAllWithSiblings(selector, range).headOption

  def code: Option[String]

  def formattedCode: Option[String]

  def text: Option[String]

  def ownText: Option[String]

  def boilerPipe: Option[String]

  def breadcrumb: Option[Seq[String]]

  def allAttr: Option[Map[String, String]]

  def attr(attr: String, noEmpty: Boolean = true): Option[String]

  // TODO: resolve by "@_href" dynamic function
  def href: Option[String]

  // TODO: resolve by "@_src" dynamic function
  def src: Option[String]
}