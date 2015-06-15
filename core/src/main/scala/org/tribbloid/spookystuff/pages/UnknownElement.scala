package org.tribbloid.spookystuff.pages

/**
 * Created by peng on 2/5/15.
 */
class UnknownElement(
                      override val uri: String
                      ) extends Unstructured {

  override def text: Option[String] = None

  override def children(selector: String): Elements[Unstructured] = new Elements(Array[Unstructured]())

  override def childrenWithSiblings(start: String, range: Range): Elements[Siblings[Unstructured]] = new Elements(Array[Siblings[Unstructured]]())

  override def code: Option[String] = None

  override def ownText: Option[String] = None

  override def boilerPipe: Option[String] = None

  override def attr(attr: String, noEmpty: Boolean): Option[String] = None}