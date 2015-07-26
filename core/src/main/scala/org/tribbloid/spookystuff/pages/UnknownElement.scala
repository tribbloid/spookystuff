package org.tribbloid.spookystuff.pages

/**
 * Created by peng on 2/5/15.
 */
class UnknownElement(
                      override val uri: String
                      ) extends Unstructured {

  override def text: Option[String] = None

  override def findAll(selector: String): Elements[Unstructured] = new Elements(List[Unstructured]())

  override def findAllWithSiblings(start: String, range: Range): Elements[Siblings[Unstructured]] = new Elements(List[Siblings[Unstructured]]())

  override def children(selector: Selector): Elements[Unstructured] = findAll(selector)

  override def childrenWithSiblings(selector: Selector, range: Range): Elements[Siblings[Unstructured]] = findAllWithSiblings(selector, range)

  override def code: Option[String] = None

  override def ownText: Option[String] = None

  override def boilerPipe: Option[String] = None

  override def attr(attr: String, noEmpty: Boolean): Option[String] = None
}