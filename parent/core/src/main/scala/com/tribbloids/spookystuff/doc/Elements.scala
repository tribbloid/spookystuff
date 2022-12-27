package com.tribbloids.spookystuff.doc

/**
  * Created by peng on 18/07/15.
  */
object Elements {

//  def newBuilder[T <: Unstructured]: mutable.Builder[T, Elements[T]] = new mutable.Builder[T, Elements[T]] {
//
//    val buffer = new mutable.ArrayBuffer[T]()
//
//    override def +=(elem: T): this.type = {
//      buffer += elem
//      this
//    }
//
//    override def result(): Elements[T] = new Elements(buffer.toList)
//
//    override def clear(): Unit = buffer.clear()
//  }

  object empty extends Elements[Nothing](Nil)
}

case class Elements[+T <: Unstructured](override val delegate: List[T]) extends Unstructured with DelegateSeq[T] {

  def uris: Seq[String] = delegate.map(_.uri)

  def codes: Seq[String] = delegate.flatMap(_.code)

  def formattedCodes: Seq[String] = delegate.flatMap(_.formattedCode)

  def allAttrs: Seq[Map[String, String]] = delegate.flatMap(_.allAttr)

  def attrs(attr: String, noEmpty: Boolean = true): Seq[String] = delegate.flatMap(_.attr(attr, noEmpty))

  def hrefs: List[String] = delegate.flatMap(_.href)

  def srcs: List[String] = delegate.flatMap(_.src)

  def texts: Seq[String] = delegate.flatMap(_.text)

  def ownTexts: Seq[String] = delegate.flatMap(_.ownText)

  def boilerPipes: Seq[String] = delegate.flatMap(_.boilerPipe)

  override def uri: String = uris.headOption.orNull

  override def text: Option[String] = texts.headOption

  override def code: Option[String] = codes.headOption

  override def formattedCode: Option[String] = formattedCodes.headOption

  override def findAll(selector: String): Elements[Unstructured] = new Elements(delegate.flatMap(_.findAll(selector)))

  override def findAllWithSiblings(selector: String, range: Range): Elements[Siblings[Unstructured]] =
    new Elements(delegate.flatMap(_.findAllWithSiblings(selector, range)))

  override def children(selector: CSSQuery): Elements[Unstructured] = new Elements(
    delegate.flatMap(_.children(selector))
  )

  override def childrenWithSiblings(selector: CSSQuery, range: Range): Elements[Siblings[Unstructured]] =
    new Elements(delegate.flatMap(_.childrenWithSiblings(selector, range)))

  override def ownText: Option[String] = ownTexts.headOption

  override def boilerPipe: Option[String] = boilerPipes.headOption

  override def allAttr: Option[Map[String, String]] = allAttrs.headOption

  override def attr(attr: String, noEmpty: Boolean): Option[String] = attrs(attr, noEmpty).headOption

  override def href: Option[String] = hrefs.headOption

  override def src: Option[String] = srcs.headOption

  override def breadcrumb: Option[Seq[String]] = delegate.head.breadcrumb
}
