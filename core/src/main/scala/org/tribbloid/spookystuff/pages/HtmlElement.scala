package org.tribbloid.spookystuff.pages

import java.nio.charset.Charset

import de.l3s.boilerpipe.extractors.ArticleExtractor
import org.jsoup.Jsoup
import org.jsoup.nodes.Element

/**
 * Created by peng on 11/30/14.
 */
object HtmlElement {

  def apply(html: String, uri: String): HtmlElement = new HtmlElement(null, html, None, uri)

  def apply(content: Array[Byte], charSet: Charset, uri: String): HtmlElement = apply(new String(content, charSet), uri)
}

class HtmlElement private (
                            @transient _parsed: Element,
                            val html: String,
                            val tag: Option[String],
                            override val uri: String
                            ) extends Unstructured {

  //constructor for HtmlElement returned by .children()
  def this(_parsed: Element) = this(
    _parsed,
    _parsed.outerHtml(),
    Some(_parsed.tagName()),
    _parsed.baseUri()
  )

  override def equals(obj: Any): Boolean = obj match {
    case other: HtmlElement =>
      (this.html == other.html) && (this.uri == other.uri)
    case _ => false
  }

  override def hashCode(): Int = (this.html, this.uri).hashCode()

  @transient lazy val parsed = Option(_parsed).getOrElse {

    tag match {
      case Some(_tag) =>
        val container = if (_tag == "tr" || _tag == "td") Jsoup.parseBodyFragment(s"<table>$html</table>", uri)
        else Jsoup.parseBodyFragment(html, uri)

        container.select(_tag).first()
      case _ =>
        Jsoup.parse(html, uri)
    }
  }

  import scala.collection.JavaConversions._

  override def children(selector: String) = new Elements(parsed.select(selector).map(new HtmlElement(_)).toList)

  override def childrenWithSiblings(selector: String, range: Range) = {

    val children = parsed.select(selector)
    val colls = children.map{
      self =>
        val selfIndex = self.elementSiblingIndex()
        //        val siblings = self.siblingElements()
        val siblings = self.parent().children()

        val prevChildIndex = siblings.lastIndexWhere(ee => children.contains(ee), selfIndex - 1)
        val head = if (prevChildIndex == -1) selfIndex + range.head
        else Math.max(selfIndex + range.head, prevChildIndex + 1)

        val nextChildIndex = siblings.indexWhere(ee => children.contains(ee), selfIndex + 1)
        val tail = if (nextChildIndex == -1) selfIndex + range.last
        else Math.min(selfIndex + range.last, nextChildIndex -1)

        val selected = siblings.slice(head,  tail + 1)

        new Siblings(selected.map(new HtmlElement(_)).toList)
    }
    new Elements(colls.toList)
  }

  override def code: Option[String] = Some(html)

  override def attr(attr: String, noEmpty: Boolean = true): Option[String] = {

    val result = parsed.attr(attr)

    if (noEmpty && result.trim.replaceAll("\u00A0", "").isEmpty) None
    else Option(result)
  }

  override def text: Option[String] = Option(parsed.text)

  override def ownText: Option[String] = Option(parsed.ownText)

  override def boilerPipe: Option[String] = {
    val result = ArticleExtractor.INSTANCE.getText(parsed.outerHtml())

    Option(result)
  }

  override def toString: String = html
}