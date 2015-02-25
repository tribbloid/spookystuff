package org.tribbloid.spookystuff.pages

import java.nio.charset.Charset

import de.l3s.boilerpipe.extractors.ArticleExtractor
import org.jsoup.Jsoup
import org.jsoup.nodes.Element

/**
 * Created by peng on 11/30/14.
 */
class HtmlElement private (
                            @transient _parsed: Element,
                            val html: String,
                            override val uri: String
                            ) extends Unstructured {

  def this(_parsed: Element) = this(
    _parsed,
    _parsed.outerHtml(),
    _parsed.baseUri()
  )

  def this(html: String, uri: String) = this(null, html, uri)

  def this(content: Array[Byte], charSet: Charset, uri: String) = this(new String(content, charSet), uri)

  override def equals(obj: Any): Boolean = obj match {
    case other: HtmlElement =>
      (this.html == other.html) && (this.uri == other.uri)
    case _ => false
  }

  override def hashCode(): Int = (this.html, this.uri).hashCode()

  @transient private lazy val parsed = Option(_parsed).getOrElse{
    val tableHtml = "<table>"+html+"</table>"
    val table = Jsoup.parse(tableHtml, uri)
    table.child(0)
  }

  import scala.collection.JavaConversions._

  override def children(selector: String): Elements[HtmlElement] = new Elements(parsed.select(selector).map(new HtmlElement(_)))

  override def rangeSelect(start: String, range: Range): Elements[Elements[HtmlElement]] = {

    val elements = parsed.select(start)
    val coll = elements.map{
      element =>
        val allSiblings = element.parent().children()
        val selfIndex = element.elementSiblingIndex()
        val selected = allSiblings.slice(selfIndex + range.head, selfIndex + range.last + 1)
//        val untilIndex = if (until == null) selected.indexWhere(_.select(start).nonEmpty, 1)
//        else selected.indexWhere(e => e.select(start).nonEmpty || e.select(until).nonEmpty, 1)
//        val deoverlapped = selected.slice(0, untilIndex)

        new Elements(selected.map(new HtmlElement(_)))
    }
    new Elements(coll)
  }

  override def markup: Option[String] = Some(html)

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