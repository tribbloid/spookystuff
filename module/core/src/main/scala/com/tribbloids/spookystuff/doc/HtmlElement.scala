package com.tribbloids.spookystuff.doc

import de.l3s.boilerpipe.extractors.ArticleExtractor
import org.jsoup.Jsoup
import org.jsoup.nodes.Element

/**
  * Created by peng on 11/30/14.
  */
object HtmlElement {

  def breadcrumb(e: Element): Seq[String] = {

    import scala.jdk.CollectionConverters.*

    e.parents().asScala.toSeq.map(_.tagName()).reverse :+ e.tagName()
  }

  def apply(html: String, uri: String): HtmlElement = new HtmlElement(null, html, None, uri)

}

class HtmlElement private (
    @transient val _parsed: Element,
    val html: String,
    val tag: Option[String],
    val uri: String
) extends Unstructured {

  import scala.jdk.CollectionConverters.*

  // constructor for HtmlElement returned by .children()
  private def this(_parsed: Element) = this(
    _parsed, {
      _parsed.ownerDocument().outputSettings().prettyPrint(false)
      _parsed.outerHtml()
    },
    Some(_parsed.tagName()),
    _parsed.baseUri()
  )

  override def equals(obj: Any): Boolean = obj match {
    case other: HtmlElement =>
      (this.html == other.html) && (this.uri == other.uri)
    case _ => false
  }

  override def hashCode(): Int = (this.html, this.uri).hashCode()

  @transient lazy val parsed: Element = Option(_parsed).getOrElse {

    tag match {
      case Some(_tag) =>
        val container =
          if (_tag == "tr" || _tag == "td") Jsoup.parseBodyFragment(s"<table>$html</table>", uri)
          else Jsoup.parseBodyFragment(html, uri)

        container.select(_tag).first()
      case _ =>
        Jsoup.parse(html, uri)
    }
  }

  override def findAll(selector: DocSelector): Elements[HtmlElement] =
    Elements(
      parsed
        .select(selector.toString)
        .asScala
        .map(new HtmlElement(_))
        .toList
    )

  override def findAllWithSiblings(selector: DocSelector, range: Range): Elements[Siblings[HtmlElement]] = {

    val found = parsed.select(selector.toString).asScala.toSeq
    expand(found, range)
  }

  private def expand(found: Seq[Element], range: Range) = {
    val colls = found.map { self =>
      val selfIndex = self.elementSiblingIndex()
      //        val siblings = self.siblingElements()
      val siblings = self.parent().children().asScala

      val prevChildIndex = siblings.lastIndexWhere(ee => found.contains(ee), selfIndex - 1)
      val head =
        if (prevChildIndex == -1) selfIndex + range.head
        else Math.max(selfIndex + range.head, prevChildIndex + 1)

      val nextChildIndex = siblings.indexWhere(ee => found.contains(ee), selfIndex + 1)
      val tail =
        if (nextChildIndex == -1) selfIndex + range.last
        else Math.min(selfIndex + range.last, nextChildIndex - 1)

      val selected = siblings.slice(head, tail + 1)

      new Siblings(selected.map(new HtmlElement(_)).toList)
    }
    Elements(colls.toList)
  }

  override def children(selector: DocSelector): Elements[HtmlElement] = {

    val found: Seq[Element] = parsed
      .select(selector.toString)
      .asScala
      .toSeq
      .filter(elem => parsed.children().contains(elem)) // TODO: switch to more efficient NodeFilter
    Elements(found.map(new HtmlElement(_)).toList)
  }

  override def childrenWithSiblings(selector: DocSelector, range: Range): Elements[Siblings[Unstructured]] = {

    val found: Seq[Element] = parsed
      .select(selector.toString)
      .asScala
      .toSeq
      .filter(elem => parsed.children().contains(elem)) // TODO: ditto
    expand(found, range)
  }

  override def code: Option[String] = {
    Some(html)
  }

  override def formattedCode: Option[String] = {
    parsed.ownerDocument().outputSettings().prettyPrint(true)
    Some(parsed.outerHtml())
  }

  override def allAttr: Option[Map[String, String]] = {
    val result = Map(parsed.attributes.asScala.toSeq.map { attr =>
      attr.getKey -> attr.getValue
    }*)
    Some(result)
  }

  override def attr(attr: String, noEmpty: Boolean = true): Option[String] = {

    val result = parsed.attr(attr)

    if (noEmpty && result.trim.replaceAll("\u00A0", "").isEmpty) None
    else Option(result)
  }

  override def href: Option[String] =
    attr(
      "abs:href"
    ) // TODO: if this is identical to the uri itself, it should be considered an inline/invalid link and have None output

  override def src: Option[String] = attr("abs:src")

  override def text: Option[String] = Option(parsed.text)

  override def ownText: Option[String] = Option(parsed.ownText)

  override def boilerPipe: Option[String] = {
    val result = ArticleExtractor.INSTANCE.getText(html)

    Option(result)
  }

  override def toString: String = html

  override def breadcrumb: Option[Seq[String]] = Some(HtmlElement.breadcrumb(this.parsed))
}
