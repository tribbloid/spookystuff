package org.tribbloid.spookystuff.pages

import java.nio.charset.Charset

import de.l3s.boilerpipe.extractors.ArticleExtractor
import org.jsoup.Jsoup
import org.jsoup.nodes.Element

/**
 * Created by peng on 11/30/14.
 */
object HtmlElement {

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

  //constructor for HtmlElemtn returned by
  def this(html: String, uri: String) = this(null, html, None, uri)

  def this(content: Array[Byte], charSet: Charset, uri: String) = this(new String(content, charSet), uri)

  override def equals(obj: Any): Boolean = obj match {
    case other: HtmlElement =>
      (this.html == other.html) && (this.uri == other.uri)
    case _ => false
  }

  override def hashCode(): Int = (this.html, this.uri).hashCode()

  private def fragmentContainer = Jsoup.parseBodyFragment("<table></table>", uri)

  @transient lazy val parsed = Option(_parsed).getOrElse {

    tag match {
      case Some(ttag) =>
        val container = if (ttag == "tr" || ttag == "td") Jsoup.parseBodyFragment(s"<table>$html</table>", uri)
        else Jsoup.parseBodyFragment(html, uri)

        container.select(ttag).first()
      case _ =>
        Jsoup.parse(html, uri)
    }
  }

  import scala.collection.JavaConversions._

  override def children(selector: String) = new Elements(parsed.select(selector).map(new HtmlElement(_)).toArray)

  override def childrenWithSiblings(start: String, range: Range) = {

    val children = parsed.select(start)
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

        new Siblings(selected.map(new HtmlElement(_)).toArray)
    }
    new Elements(colls.toArray)
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

//object HtmlElementSerializer extends Serializer[HtmlElement] {
//
//  override def deserialize(implicit format: Formats): PartialFunction[(TypeInfo, JValue), HtmlElement] = ??? //TODO: how to support it?
//
//  override def serialize(implicit format: Formats): PartialFunction[Any, JValue] = {
//    case element: HtmlElement =>
//      JString(element.html)
//  }
//}