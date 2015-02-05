package org.tribbloid.spookystuff.pages

/**
 * Created by peng on 2/5/15.
 */
class UnknownElement(
                      override val uri: String
                      ) extends Unstructured {

  override def text: Option[String] = None

  override def children(selector: String): Seq[Unstructured] = Seq()

  override def markup: Option[String] = None

  override def ownText: Option[String] = None

  override def boilerPipe(): Option[String] = None

  override def attr(attr: String, noEmpty: Boolean): Option[String] = None
}