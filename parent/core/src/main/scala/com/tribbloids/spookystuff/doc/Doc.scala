package com.tribbloids.spookystuff.doc

import ai.acyclic.prover.commons.same.EqualBy
import com.tribbloids.spookystuff._
import com.tribbloids.spookystuff.caching.DocCacheLevel
import com.tribbloids.spookystuff.doc.Content.InMemoryBlob
import com.tribbloids.spookystuff.doc.Observation.DocUID
import com.tribbloids.spookystuff.doc.Unstructured.Unrecognisable
import com.tribbloids.spookystuff.utils.CommonUtils
import com.tribbloids.spookystuff.utils.io.ResourceMetadata
import com.tribbloids.spookystuff.utils.serialization.NOTSerializable
import org.apache.commons.csv.CSVFormat
import org.apache.hadoop.shaded.org.apache.http.StatusLine
import org.apache.hadoop.shaded.org.apache.http.entity.ContentType
import org.apache.tika.io.TikaInputStream
import org.apache.tika.metadata.{Metadata, TikaCoreProperties}
import org.mozilla.universalchardet.UniversalDetector

import scala.language.implicitConversions

object Doc {

  val CONTENT_TYPE: String = "contentType"
  val CSV_FORMAT: String = "csvFormat"

  val defaultCSVFormat: CSVFormat = CSVFormat.DEFAULT

  implicit def asContent(v: Doc): Content = v.content
}

@SerialVersionUID(94865098324L)
case class Doc(
    override val uid: DocUID,
    uri: String, // redirected
    declaredContentType: Option[String] = None,
    //                 cookie: Seq[SerializableCookie] = Nil,
    override val timeMillis: Long = System.currentTimeMillis(),
    override val cacheLevel: DocCacheLevel.Value = DocCacheLevel.All,
    httpStatus: Option[StatusLine] = None,
    metadata: ResourceMetadata = ResourceMetadata.empty
)(
    var content: Content = null
) extends Observation.Success
    with EqualBy {

  @transient lazy val samenessDelegatedTo: Any = (uid, uri, metadata, timeMillis, httpStatus.toString)

  override def updated(
      uid: DocUID = this.uid,
      cacheLevel: DocCacheLevel.Value = this.cacheLevel
  ): Doc.this.type = this.copy(uid = uid, cacheLevel = cacheLevel)(content).asInstanceOf[this.type]

  def withMetadata(tuples: (String, Any)*): Doc = {

    this.copy(
      metadata = this.metadata ++: ResourceMetadata.From.tuple(tuples: _*)
    )(content)
  }

  def setRaw(raw: Array[Byte]): this.type = {

    object ContentTypeDetection extends NOTSerializable {

      lazy val declaredOpt: Option[CSSQuery] = {
        metadata.`content-type`.get
          .map(v => "" + v) // TODO: why?
          .orElse(declaredContentType)
      }

      lazy val detected: ContentType = declaredOpt match {
        case Some(str) =>
          val result = ContentType.parse(str)
          result

        case None =>
          val metadata = new Metadata()
          val slash: Int = uri.lastIndexOf('/')
          metadata.set(TikaCoreProperties.RESOURCE_NAME_KEY, uri.substring(slash + 1))
          val stream = TikaInputStream.get(raw, metadata)
          try {
            val mediaType = Const.tikaDetector.detect(stream, metadata)
            //        val mimeType = mediaType.getBaseType.toString
            //        val charset = new CharsetDetector().getString(content, null)
            //        ContentType.create(mimeType, charset)

            val str = mediaType.toString
            val result = ContentType.parse(str)
            result

          } finally {
            stream.close()
          }
      }

      lazy val output: ContentType = {

        if (detected.getCharset == null) {

          val charsetDetecter = new UniversalDetector(null)
          val ss = 4096

          for (i <- 0.until(raw.length, ss)) {
            val length = Math.min(raw.length - i, ss)
            charsetDetecter.handleData(raw, i, length)
            if (charsetDetecter.isDone) charsetDetecter.getDetectedCharset
          }

          charsetDetecter.dataEnd()
          val charSetOpt = Option(charsetDetecter.getDetectedCharset)

          val charSet: String = charSetOpt.getOrElse {

            if (detected.getMimeType.contains("text")) Const.defaultTextCharset
            else if (detected.getMimeType.contains("application")) Const.defaultApplicationCharset
            else Const.defaultApplicationCharset
          }

          detected.withCharset(charSet)
        } else detected
      }
    }

    this.content = Content.Original(InMemoryBlob(raw), ContentTypeDetection.output)
    this
  }

  // TODO: use reflection to find any element implementation that can resolve supplied MIME type
  @transient lazy val rootOpt: Option[Unstructured] = {

    val content = this.content
    import content._

    if (mimeType.contains("html") || mimeType.contains("xml") || mimeType.contains("directory")) {
      Some(HtmlElement(contentStr, uri)) // not serialize, parsing is faster
    } else if (mimeType.contains("json")) {
      Some(JsonElement(contentStr, null, uri)) // not serialize, parsing is faster
    } else if (mimeType.contains("csv")) {
      val csvFormat: CSVFormat = metadata.asMap
        .get(Doc.CSV_FORMAT)
        .map {
          case v: CSVFormat => v
          case v @ _        => CSVFormat.valueOf(v.toString)
        }
        .getOrElse(Doc.defaultCSVFormat)

      Some(CSVElement.Block.apply(contentStr, uri, csvFormat)) // not serialize, parsing is faster
    } else if (mimeType.contains("plain") || mimeType.contains("text")) {
      Some(PlainElement(contentStr, uri)) // not serialize, parsing is faster
    } else {
      None
    }
  }

  @transient lazy val converted: Observation = {
    rootOpt match {
      case Some(_) =>
        this
      case None =>
        try {
          this.copy()(
            content = this.content.converted
          )
        } catch {
          case e: Throwable =>
            ConversionError(this, e)
        }
    }
  }

  override type RootType = Unstructured
  def root: Unstructured = converted match {
    case d: Doc => d.rootOpt.getOrElse(Unrecognisable)
    case _      => Unrecognisable
  }

  def saved: Seq[CSSQuery] = content.blob.saved

  case class save(
      spooky: SpookyContext,
      overwrite: Boolean = false
  ) {

    def apply(
        pathParts: Seq[String]
    ): Content = {

      val path = CommonUtils.\\\(pathParts: _*)

      def wCtx = content.withCtx(spooky)

      val saved = wCtx.save1(path, overwrite)
      Doc.this.content = saved
      saved
    }

    def auditing(): Content = {
      apply(spooky.dirConf.auditing :: spooky.conf.auditingFileStructure(Doc.this) :: Nil)

    }

    private lazy val errorDumpRoot =
      if (Doc.this.isImage) spooky.dirConf.errorScreenshot
      else spooky.dirConf.errorDump

    // TODO: merge into cascade retries
    def errorDump(): Content = {

      apply(errorDumpRoot :: spooky.conf.errorDumpFileStructure(Doc.this) :: Nil)

    }

    def errorDumpLocally(): Content = {

      apply(
        errorDumpRoot :: spooky.conf.errorDumpFileStructure(Doc.this) :: Nil
      )
    }
  }

  override def docForAuditing: Option[Doc] = Some(this)
}
