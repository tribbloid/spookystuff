package com.tribbloids.spookystuff.doc

import ai.acyclic.prover.commons.multiverse.Projection
import ai.acyclic.prover.commons.spark.serialization.NOTSerializable
import ai.acyclic.prover.commons.util.PathMagnet
import com.tribbloids.spookystuff.*
import com.tribbloids.spookystuff.caching.DocCacheLevel
import com.tribbloids.spookystuff.commons.data.Magnets.AttrValueMag
import com.tribbloids.spookystuff.doc.Content.InMemoryBlob
import com.tribbloids.spookystuff.doc.Error.ConversionError
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
import scala.util.{Success, Try}

object Doc {

  val CONTENT_TYPE: String = "contentType"
  val CSV_FORMAT: String = "csvFormat"

  val defaultCSVFormat: CSVFormat = CSVFormat.DEFAULT

  implicit def _asContent(v: Doc): Content = v._content

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
    private var _content: Content = null
) extends Observation.Success
    with Projection.Equals {

  import Doc.*

  @transient lazy val samenessKey: Any = (uid, uri, metadata, timeMillis, httpStatus.toString)

  def content: Content = _content

  override def updated(
      uid: DocUID = this.uid,
      cacheLevel: DocCacheLevel.Value = this.cacheLevel
  ): Doc = this.copy(uid = uid, cacheLevel = cacheLevel)(_content)

  def withMetadata(tuples: (String, Any)*): Doc = {

    this.copy(
      metadata = this.metadata ++: ResourceMetadata(tuples.map(v => AttrValueMag.fromKeyValue(v))*)
    )(_content)
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

    this._content = Content.Original(
      new InMemoryBlob(raw),
      ContentTypeDetection.output
    )
    this
  }

  // TODO: use compile-time summoning to find an element implementation that can resolve supplied MIME type
  @transient lazy val rootOpt: Option[Unstructured] = {

    val content = this._content
    import content.*

    if (mimeType.contains("html") || mimeType.contains("xml") || mimeType.contains("directory")) {
      Some(HtmlElement(contentStr, uri)) // not serialize, parsing is faster
    } else if (mimeType.contains("json")) {
      Some(JsonElement(contentStr, null)) // not serialize, parsing is faster
    }
//    else if (mimeType.contains("csv")) {
    // TODO: disabled, will delegate to Univocity or Apache Tika later
    //  Apache commons-csv is very brittle and slow
//X
//      val formatV = metadata.lookup.get(Doc.CSV_FORMAT)
//
//      val csvFormat: CSVFormat = formatV
//        .map {
//          case v: String =>
//            CSVFormat.valueOf(v)
//          case v =>
//            throw new UnsupportedOperationException(s"expecting CSV format of String type, got ${v.getClass}")
//        }
//        .getOrElse(Doc.defaultCSVFormat)
//
//      val reader = new StringReader(contentStr)
//
//      Some(CSVElement.Block.apply(reader, uri, csvFormat)) // not serialize, parsing is faster
//    }
    else if (mimeType.contains("plain") || mimeType.contains("text")) {
      Some(PlainElement(contentStr)) // not serialize, parsing is faster
    } else {
      None
    }
  }

  @transient lazy val converted: Observation = {
    Try(rootOpt) match {
      case Success(Some(_)) =>
        this
      case _ =>
        try {
          this.copy()(
            _content = this._content.normalised
          )
        } catch {
          case e: Throwable =>
            ConversionError(this, cause = e)
        }
    }
  }

  override type RootType = Unstructured
  def root: Unstructured = converted match {
    case d: Doc => d.rootOpt.getOrElse(Unrecognisable)
    case _      => Unrecognisable
  }

  def saved: Seq[String] = _content.blob.saved

  case class prepareSave(
      spooky: SpookyContext,
      overwrite: Boolean = false
  ) {

    def save(
        path: PathMagnet.URIPath,
        extension: Option[String] = None
    ): Unit = {

      def wCtx = _content.withCtx(spooky)

      val saved = wCtx.save1(path, extension, overwrite)
      Doc.this._content = saved
    }

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

    private def _errorDump(): Unit = {

      val relative = {
        spooky.conf.errorDumpFilePaths(Doc.this)
      }

      val path = errorDumpRoot :/ relative

      save(path)
    }

    // TODO: merge into cascade retries
    def errorDump(): Unit = {
      _errorDump()
    }

    def errorDumpLocally(): Unit = {
      _errorDump()
    }
  }

  override def docForAuditing: Option[Doc] = Some(this)
}
