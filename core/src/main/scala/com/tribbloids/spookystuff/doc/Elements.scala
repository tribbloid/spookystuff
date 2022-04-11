package com.tribbloids.spookystuff.doc

import scala.collection.mutable
import scala.language.implicitConversions

/**
  * Created by peng on 18/07/15.
  */
object Elements {

  def newBuilder[T <: Unstructured]: mutable.Builder[T, Elements[T]] = new mutable.Builder[T, Elements[T]] {

    val buffer = new mutable.ArrayBuffer[T]()

    override def +=(elem: T): this.type = {
      buffer += elem
      this
    }

    override def result(): Elements[T] = new Elements(buffer.toList)

    override def clear(): Unit = buffer.clear()
  }

  object empty extends Elements[Nothing](Nil)
}

class Elements[+T <: Unstructured](val seq: List[T]) extends Unstructured with HasSeq[T] {

  def uris: Seq[String] = seq.map(_.uri)

  def codes: Seq[String] = seq.flatMap(_.code)

  def formattedCodes: Seq[String] = seq.flatMap(_.formattedCode)

  def allAttrs: Seq[Map[String, String]] = seq.flatMap(_.allAttr)

  def attrs(attr: String, noEmpty: Boolean = true): Seq[String] = seq.flatMap(_.attr(attr, noEmpty))

  def hrefs: List[String] = seq.flatMap(_.href)

  def srcs: List[String] = seq.flatMap(_.src)

  def texts: Seq[String] = seq.flatMap(_.text)

  def ownTexts: Seq[String] = seq.flatMap(_.ownText)

  def boilerPipes: Seq[String] = seq.flatMap(_.boilerPipe)

  override def uri: String = uris.headOption.orNull

  override def text: Option[String] = texts.headOption

  override def code: Option[String] = codes.headOption

  override def formattedCode: Option[String] = formattedCodes.headOption

  override def findAll(selector: String) = new Elements(seq.flatMap(_.findAll(selector)))

  override def findAllWithSiblings(selector: String, range: Range) =
    new Elements(seq.flatMap(_.findAllWithSiblings(selector, range)))

  override def children(selector: CSSQuery) = new Elements(seq.flatMap(_.children(selector)))

  override def childrenWithSiblings(selector: CSSQuery, range: Range) =
    new Elements(seq.flatMap(_.childrenWithSiblings(selector, range)))

  override def ownText: Option[String] = ownTexts.headOption

  override def boilerPipe: Option[String] = boilerPipes.headOption

  override def allAttr: Option[Map[String, String]] = allAttrs.headOption

  override def attr(attr: String, noEmpty: Boolean): Option[String] = attrs(attr, noEmpty).headOption

  override def href: Option[String] = hrefs.headOption

  override def src: Option[String] = srcs.headOption

  override def breadcrumb: Option[Seq[String]] = seq.head.breadcrumb
}
