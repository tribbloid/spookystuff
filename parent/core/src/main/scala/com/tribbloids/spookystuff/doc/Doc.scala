package com.tribbloids.spookystuff.doc

import ai.acyclic.prover.commons.multiverse.Projection
import ai.acyclic.prover.commons.spark.serialization.NOTSerializable
import ai.acyclic.prover.commons.util.PathMagnet
import com.tribbloids.spookystuff.*
import com.tribbloids.spookystuff.caching.DocCacheLevel
import com.tribbloids.spookystuff.commons.data.Magnets.AttrValueMag
import com.tribbloids.spookystuff.doc.Content.InMemoryBlob
import com.tribbloids.spookystuff.doc.Observation.DocUID
import com.tribbloids.spookystuff.doc.Unstructured.Unrecognisable
import com.tribbloids.spookystuff.io.ResourceMetadata
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

  val defaultTextCharset: String = "ISO-8859-1"
  val defaultApplicationCharset: String = "UTF-8"
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
    with Projection.Equals {

  import Doc.*

  @transient lazy val samenessKey: Any = (uid, uri, metadata, timeMillis, httpStatus.toString)

  override def updated(
      uid: DocUID = this.uid,
      cacheLevel: DocCacheLevel.Value = this.cacheLevel
  ): Doc.this.type = this.copy(uid = uid, cacheLevel = cacheLevel)(content).asInstanceOf[this.type]

  def withMetadata(tuples: (String, Any)*): Doc = {

    this.copy(
      metadata = this.metadata ++: ResourceMetadata(tuples.map(v => AttrValueMag.fromKeyValue(v))*)
    )(content)
  }

  def setRaw(raw: Array[Byte]): this.type = {

    object ContentTypeDetection extends NOTSerializable {

      lazy val declaredOpt: Option[String] = {
        metadata.ContentType.get
          .map(v => "" + v) // TODO: why?
          .orElse(declaredContentType)
      }

      lazy val detectedUsingTika: ContentType = declaredOpt match {
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

      lazy val detected: ContentType = {

        if (detectedUsingTika.getCharset == null) {

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

            if (detectedUsingTika.getMimeType.contains("text")) defaultTextCharset
            else if (detectedUsingTika.getMimeType.contains("application")) defaultApplicationCharset
            else defaultApplicationCharset
          }

          detectedUsingTika.withCharset(charSet)
        } else {
          detectedUsingTika
        }
      }

      lazy val output: ContentTypeView = {
        ContentTypeView(detected)
      }
    }

    this.content = Content.Original(
      new InMemoryBlob(raw),
      ContentTypeDetection.output
    )
    this
  }

  // TODO: use compile-time summoning to find an element implementation that can resolve supplied MIME type
  @transient lazy val rootOpt: Option[Unstructured] = {

    val content = this.content
    import content.*

    if (mimeType.contains("html") || mimeType.contains("xml") || mimeType.contains("directory")) {
      Some(HtmlElement(contentStr, uri)) // not serialize, parsing is faster
    } else if (mimeType.contains("json")) {
      Some(JsonElement(contentStr, null, uri)) // not serialize, parsing is faster
    } else if (mimeType.contains("csv")) {
      val csvFormat: CSVFormat = metadata.lookup
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

  @transient lazy val normalised: Observation = {
    rootOpt match {
      case Some(_) =>
        this
      case None =>
        try {
          this.copy()(
            content = this.content.normalised
          )
        } catch {
          case e: Throwable =>
            ConversionError(this, e)
        }
    }
  }

  override type RootType = Unstructured
  def root: Unstructured = normalised match {
    case d: Doc => d.rootOpt.getOrElse(Unrecognisable)
    case _      => Unrecognisable
  }

  def saved: Seq[String] = content.blob.saved

  case class prepareSave(
      spooky: SpookyContext,
      overwrite: Boolean = false
  ) {

    def save(
        path: PathMagnet.URIPath,
        extension: Option[String] = None
    ): Unit = {

      def wCtx = content.withCtx(spooky)

      val saved = wCtx.save1(path, extension, overwrite)
      Doc.this.content = saved
    }

//    def apply(pathParts: Seq[String]): Content = as(pathParts)

    def auditing(): Unit = {
      val uri = PathMagnet.URIPath(spooky.dirConf.auditing) :/
        spooky.conf.auditingFilePaths(Doc.this)
      save(uri)
    }

    private lazy val errorDumpRoot = {
      val str =
        if (Doc.this.isImage) spooky.dirConf.errorScreenshot
        else spooky.dirConf.errorDump

      PathMagnet.URIPath(str)
    }

    // TODO: merge into cascade retries
    def errorDump(): Unit = {

      save(errorDumpRoot :/ spooky.conf.errorDumpFilePaths(Doc.this))

    }

    def errorDumpLocally(): Unit = {

      save(
        errorDumpRoot :/ spooky.conf.errorDumpFilePaths(Doc.this)
      )
    }
  }

  override def docForAuditing: Option[Doc] = Some(this)
}
