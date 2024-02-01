package com.tribbloids.spookystuff.doc

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.utils.TreeThrowable
import com.tribbloids.spookystuff.utils.serialization.NOTSerializable
import org.apache.hadoop.fs.{FSDataOutputStream, Path}
import org.apache.hadoop.shaded.org.apache.http.entity.ContentType
import org.apache.tika.io.TikaInputStream
import org.apache.tika.metadata.{HttpHeaders, Metadata}
import org.apache.tika.parser.{AutoDetectParser, ParseContext}
import org.apache.tika.sax.ToHTMLContentHandler

import java.nio.charset.Charset
import java.util.UUID

sealed trait Content extends SpookyContext.CanRunWith {

  import Content._

  def blob: Blob

  case class _WithCtx(ctx: SpookyContext) extends NOTSerializable {

    lazy val data: Array[Byte] = blob.load(ctx)

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
          os.write(data) //       remember that apache IOUtils is defective for DFS!

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
    ): DFSSavedBlob = {

      val absolutePath = doSave1(path, overwrite).toString

      blob match {
        case v: InMemoryBlob =>
          DFSSavedBlob(v, absolutePath)
        case v: DFSSavedBlob =>
          v.withNewPath(absolutePath)
          v.copy(
            paths = v.paths + (absolutePath -> v.paths.size)
          )
      }
    }

    lazy val parsed: Parsed = {

      Content.this match {
        case v: Parsed => v
        case v: Raw =>
          val handler = new ToHTMLContentHandler()

          val metadata = new Metadata()
          val stream = TikaInputStream.get(data, metadata)
          val html: String =
            try {
              metadata.set(HttpHeaders.CONTENT_ENCODING, blob.charSet.name())
              metadata.set(HttpHeaders.CONTENT_TYPE, v.mimeType)
              val parser = new AutoDetectParser()
              val context = new ParseContext()
              parser.parse(stream, handler, metadata, context)
              handler.toString
            } finally {
              stream.close()
            }

          Parsed(InMemoryBlob(html.getBytes(v.preferredCharset)))
      }
    }
  }
}

object Content {

  sealed trait Blob {

    def load(ctx: SpookyContext): Array[Byte]

    def charSet: Charset

  }

  case class InMemoryBlob(
      raw: Array[Byte],
      charSet: Charset = Charset.defaultCharset()
  ) extends Blob {

    override def load(ctx: SpookyContext): Array[Byte] = {
      raw
    }
  }

  case class DFSSavedBlob(
      @transient original: InMemoryBlob,
      path1: String,
      paths: Map[String, Int] = Map.empty // DFS path -> precedence
  ) extends Blob {
    // can reconstruct actual content from any of the paths

    final override def charSet: Charset = original.charSet

    def withNewPath(v: String): DFSSavedBlob = {
      val oldSize = paths.size
      this.copy(
        paths = paths + (v -> oldSize)
      )
    }

    override def load(ctx: SpookyContext): Array[Byte] = {

      val sortedPath = Seq(path1) ++ paths.toSeq.sortBy(_._2).map(_._1)

      val trials = sortedPath.map { path => () =>
        DocUtils.load(path)(ctx)
      }

      val result = TreeThrowable.|||^(trials)
      result.get
    }
  }

  case class Raw(
      blob: Blob,
      // can be different from bytes.charSet, will transcode on demand
      contentType: ContentType,
      transcode: Option[Charset] = None
  ) extends Content {

    @transient def mimeType: String = contentType.getMimeType

    @transient lazy val preferredCharset: Charset = transcode.getOrElse(
      blob.charSet
    )
  }

  case class Parsed(
      blob: Blob
      // mimeType is always text/html
      // preferredCharset is always bytes.charSet
  ) extends Content {}
}
