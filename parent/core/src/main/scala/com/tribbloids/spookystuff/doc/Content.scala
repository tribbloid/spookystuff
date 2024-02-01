package com.tribbloids.spookystuff.doc

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.utils.TreeThrowable
import com.tribbloids.spookystuff.utils.io.HDFSResolver
import com.tribbloids.spookystuff.utils.serialization.NOTSerializable
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.{FSDataOutputStream, Path}
import org.apache.hadoop.shaded.org.apache.http.entity.ContentType
import org.apache.tika.io.TikaInputStream
import org.apache.tika.metadata.{HttpHeaders, Metadata}
import org.apache.tika.mime.{MimeType, MimeTypes}
import org.apache.tika.parser.{AutoDetectParser, ParseContext}
import org.apache.tika.sax.ToHTMLContentHandler

import java.nio.charset.Charset
import java.util.UUID

sealed trait Content extends SpookyContext.CanRunWith with Serializable {

  import Content._

  import scala.jdk.CollectionConverters._

  def blob: Blob
  def withBlob(blob: Blob): Content

  def contentType: ContentTypeView

  @transient lazy val charsetOpt: Option[Charset] = Option(contentType.getCharset)

  def charset: Charset = charsetOpt.getOrElse(defaultCharset)

  @transient lazy val contentStr = new String(blob.raw, charset)

  @transient lazy val mimeType: String = contentType.getMimeType

  def isImage: Boolean = mimeType.startsWith("image")

  lazy val tikaMimeType: MimeType = MimeTypes.getDefaultMimeTypes.forName(mimeType)
  lazy val fileExtensions: Seq[String] = tikaMimeType.getExtensions.asScala.toSeq.map { str =>
    if (str.startsWith(".")) str.splitAt(1)._2
    else str
  }

  def defaultFileExtension: Option[String] = {
    fileExtensions.headOption
  }

  @transient lazy val converted: Converted = {

    Content.this match {
      case v: Converted =>
        v
      case v: Original =>
        val handler = new ToHTMLContentHandler()

        val metadata = new Metadata()
        val stream = TikaInputStream.get(blob.raw, metadata)
        val result = {
          try {
            metadata.set(HttpHeaders.CONTENT_ENCODING, charsetOpt.map(_.name()).orNull)
            metadata.set(HttpHeaders.CONTENT_TYPE, v.mimeType)
            val parser = new AutoDetectParser()
            val context = new ParseContext()
            parser.parse(stream, handler, metadata, context)
            val html = handler.toString

            Converted(
              new InMemoryBlob(html.getBytes(v.preferredTranscode)),
              ContentTypeView(
                ContentType.TEXT_HTML.withCharset(v.preferredTranscode)
              )
            )
          } finally {
            stream.close()
          }
        }
        result

    }
  }

  case class _WithCtx(ctx: SpookyContext) extends NOTSerializable {

    lazy val raw: Array[Byte] = blob.raw

    def doSave1(
        path: String,
        overwrite: Boolean = false
    ): Path = { // return absolute path

      DocUtils.dfsWrite("save", path, ctx) { progress =>
        //          val resolver = ctx.pathResolver // TODO: simplify using this

        val fullPath = new Path(path)
        val fs = fullPath.getFileSystem(ctx.hadoopConf)
        //      if (!overwrite && fs.exists(fullPath)) fullPath = new Path(path + "-" + UUID.randomUUID())
        val (fos: FSDataOutputStream, actualPath: Path) =
          try {
            fs.create(fullPath, overwrite) -> fullPath
          } catch {
            case _: Exception =>
              val altPath = new Path(path + "-" + UUID.randomUUID())
              fs.create(altPath, overwrite) -> altPath
          }

        val os = progress.WrapOStream(fos)

        try {
          os.write(raw) //       remember that apache IOUtils is defective for DFS!

          val metrics = ctx.spookyMetrics
          metrics.saved += 1
          //        metrics.savedPath.add(path -> progress.indicator.longValue())

          val absolutePath = fs.resolvePath(actualPath)
          absolutePath

        } finally {
          os.close()
        }
      }
    }

    def save1(
        path: String,
        overwrite: Boolean = false
    ): Content = {

      val absolutePath = doSave1(path, overwrite).toString

      val newBlob = blob match {
        case v: InMemoryBlob =>
          DFSSavedBlob(v, ctx.pathResolver, absolutePath)
        case v: DFSSavedBlob =>
          v.addPath(absolutePath)
          v.copy(
            paths = v.paths + (absolutePath -> v.paths.size)
          )
      }

      Content.this.withBlob(newBlob)
    }

  }

}

object Content {

  lazy val defaultCharset: Charset = Charset.defaultCharset()
//  lazy val HTML_Default: ContentType = ContentType.TEXT_HTML.withCharset(defaultCharset)

  sealed trait Blob extends Serializable {

    def raw: Array[Byte]
    def saved: Seq[String] = Nil
  }

  class InMemoryBlob(
      val raw: Array[Byte]
  ) extends Blob {}

  case class DFSSavedBlob(
      @transient original: InMemoryBlob,
      pathResolver: HDFSResolver,
      path1: String,
      paths: Map[String, Int] = Map.empty // DFS path -> precedence
  ) extends Blob {
    // can reconstruct actual content from any of the paths

    def addPath(v: String): DFSSavedBlob = {
      val oldSize = paths.size
      this.copy(
        paths = paths + (v -> oldSize)
      )
    }

    @transient override lazy val saved: Seq[String] = Seq(path1) ++ paths.toSeq.sortBy(_._2).map(_._1)

    @transient override lazy val raw: Array[Byte] = {

      Option(original)
        .map(_.raw)
        .getOrElse {

          val trials = saved.map { path => () =>
            val result = pathResolver.input(path) { in =>
              IOUtils.toByteArray(in.stream)
            }

            result
          }

          val result = TreeThrowable.|||^(trials)
          result.get
        }
    }

  }

  case class Original(
      blob: Blob,
      // can be different from bytes.charSet, will transcode on demand
      contentType: ContentTypeView,
      transcodeOpt: Option[String] = None
  ) extends Content {

    def withBlob(blob: Blob): Content = this.copy(blob = blob)

    @transient lazy val preferredTranscode: Charset = transcodeOpt
      .map { v =>
        Charset.forName(v)
      }
      .getOrElse(
        charset
      )
  }

  case class Converted(
      blob: Blob,
      contentType: ContentTypeView
  ) extends Content {

    def withBlob(blob: Blob): Content = this.copy(blob = blob)

    override def defaultFileExtension: Option[String] = {
      fileExtensions.headOption.map { v =>
        s"converted.$v"
      }
    }
  }

}
