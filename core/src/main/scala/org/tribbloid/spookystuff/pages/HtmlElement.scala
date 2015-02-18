package org.tribbloid.spookystuff.pages

import java.nio.charset.Charset

import de.l3s.boilerpipe.extractors.ArticleExtractor
import org.jsoup.Jsoup
import org.jsoup.nodes.{Document, Element}

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
      if (_parsed.isInstanceOf[Document]) _parsed.outerHtml()
      else "<table>"+_parsed.outerHtml()+"</table>",
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

   @transient private lazy val parsed = Option(_parsed).getOrElse(Jsoup.parse(html, uri))

   import scala.collection.JavaConversions._

   override def children(selector: String): Seq[HtmlElement] = parsed.select(selector).map(new HtmlElement(_))

   override def markup: Option[String] = Some(html)

   override def attr(attr: String, noEmpty: Boolean = true): Option[String] = {
      val result = parsed.attr(attr)

      if (noEmpty && result.trim.replaceAll("\u00A0", "").isEmpty) None
      else Option(result)
   }

   override def text: Option[String] = Option(parsed.text)

   override def ownText: Option[String] = Option(parsed.ownText)

   override def boilerPipe(): Option[String] = {
      val result = ArticleExtractor.INSTANCE.getText(parsed.outerHtml())

      Option(result)
   }

   override def toString: String = html
}