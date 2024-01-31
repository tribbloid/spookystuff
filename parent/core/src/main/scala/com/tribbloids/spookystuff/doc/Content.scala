package com.tribbloids.spookystuff.doc

import org.apache.hadoop.shaded.org.apache.http.entity.ContentType
import org.apache.tika.io.TikaInputStream
import org.apache.tika.metadata.{HttpHeaders, Metadata}
import org.apache.tika.parser.{AutoDetectParser, ParseContext}
import org.apache.tika.sax.ToHTMLContentHandler

import java.nio.charset.Charset

trait Content {

  import Content._

  def parsed: TikaParsed
}

object Content {

  trait Blob {

    def raw: Array[Byte]

    def charSet: Charset
  }

  case class InMemoryBlob(
      raw: Array[Byte],
      charSet: Charset = Charset.defaultCharset()
  ) extends Blob {}

  case class DFSSavedBlob(
      @transient original: InMemoryBlob,
      paths: List[String] = Nil
  ) extends Blob {
    // can reconstruct actual content from any of the paths

    final override val charSet = original.charSet

    @transient override lazy val raw: Array[Byte] = {

      Option(original)
        .map(_.raw)
        .getOrElse {
          ???
        }
    }
  }

  case class Raw(
      bytes: Blob,
      // can be different from bytes.charSet, will transcode on demand
      contentType: ContentType,
      transcode: Option[Charset] = None
  ) extends Content {

    @transient def mimeType: String = contentType.getMimeType

    @transient lazy val preferredCharset: Charset = transcode.getOrElse(
      bytes.charSet
    )

    @transient override lazy val parsed: TikaParsed = {

      val handler = new ToHTMLContentHandler()

      val metadata = new Metadata()
      val stream = TikaInputStream.get(bytes.raw, metadata)
      val html: String =
        try {
          metadata.set(HttpHeaders.CONTENT_ENCODING, bytes.charSet.name())
          metadata.set(HttpHeaders.CONTENT_TYPE, mimeType)
          val parser = new AutoDetectParser()
          val context = new ParseContext()
          parser.parse(stream, handler, metadata, context)
          handler.toString
        } finally {
          stream.close()
        }

      TikaParsed(InMemoryBlob(html.getBytes(preferredCharset)))
    }
  }

  case class TikaParsed(
      bytes: Blob
      // mimeType is always text/html
      // preferredCharset is always bytes.charSet
  ) extends Content {

    final override def parsed: TikaParsed = this
  }
}
