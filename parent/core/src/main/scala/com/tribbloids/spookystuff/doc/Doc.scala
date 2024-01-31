package com.tribbloids.spookystuff.doc

import ai.acyclic.prover.commons.EqualBy
import com.tribbloids.spookystuff._
import com.tribbloids.spookystuff.caching.DocCacheLevel
import com.tribbloids.spookystuff.doc.Observation.DocUID
import com.tribbloids.spookystuff.utils.io.ResourceMetadata
import com.tribbloids.spookystuff.utils.CommonUtils
import org.apache.commons.csv.CSVFormat
import org.apache.hadoop.fs.Path
import org.apache.hadoop.shaded.org.apache.http.StatusLine
import org.apache.hadoop.shaded.org.apache.http.entity.ContentType
import org.apache.tika.io.TikaInputStream
import org.apache.tika.metadata.{Metadata, TikaCoreProperties}
import org.apache.tika.mime.{MimeType, MimeTypes}
import org.mozilla.universalchardet.UniversalDetector

import java.util.UUID
import scala.collection.mutable

object Doc {

  val CONTENT_TYPE: String = "contentType"
  val CSV_FORMAT: String = "csvFormat"

  val defaultCSVFormat: CSVFormat = CSVFormat.DEFAULT
}

@SerialVersionUID(94865098324L)
case class Doc(
    override val uid: DocUID,
    uri: String, // redirected
    raw: Array[Byte],
    declaredContentType: Option[String] = None,
    //                 cookie: Seq[SerializableCookie] = Nil,
    override val timeMillis: Long = System.currentTimeMillis(),
    override val cacheLevel: DocCacheLevel.Value = DocCacheLevel.All,
    httpStatus: Option[StatusLine] = None,
    override val metadata: ResourceMetadata =
      ResourceMetadata.empty, // for customizing parsing TODO: remove, delegate to CSVElement.
    saved: mutable.Set[String] = mutable.Set() // TODO: move out of constructor
) extends Observation.Success
    with EqualBy {

  import scala.jdk.CollectionConverters._

  lazy val samenessDelegatedTo: Any = (uid, uri, declaredContentType, timeMillis, httpStatus.toString)

  override def updated(
      uid: DocUID = this.uid,
      cacheLevel: DocCacheLevel.Value = this.cacheLevel
  ): Doc.this.type = this.copy(uid = uid, cacheLevel = cacheLevel).asInstanceOf[this.type]

  private def detectCharset(contentType: ContentType): String = {
    val charsetD = new UniversalDetector(null)
    val ss = 4096

    for (i <- 0.until(raw.length, ss)) {
      val length = Math.min(raw.length - i, ss)
      charsetD.handleData(raw, i, length)
      if (charsetD.isDone) return charsetD.getDetectedCharset
    }

    charsetD.dataEnd()
    val detected = charsetD.getDetectedCharset

    if (detected == null) {
      if (contentType.getMimeType.contains("text")) Const.defaultTextCharset
      else if (contentType.getMimeType.contains("application")) Const.defaultApplicationCharset
      else Const.defaultApplicationCharset
    } else detected
  }

  @transient lazy val parsedContentType: ContentType = {

    val strOpt = metadata.`content-type`.get
      .map("" + _)
      .orElse(declaredContentType)
    strOpt match {
      case Some(str) =>
        val ct = ContentType.parse(str)
        if (ct.getCharset == null) {

          ct.withCharset(detectCharset(ct))
        } else ct
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
          if (result.getCharset == null) {

            result.withCharset(detectCharset(result))
          } else result
        } finally {
          stream.close()
        }
    }
  }

  override type RootType = Unstructured
  // TODO: use reflection to find any element implementation that can resolve supplied MIME type
  @transient override lazy val root: Unstructured = {
    val effectiveCharset = charset.orNull

    lazy val contentStr = new String(raw, effectiveCharset)
    if (mimeType.contains("html") || mimeType.contains("xml") || mimeType.contains("directory")) {
      HtmlElement(contentStr, uri) // not serialize, parsing is faster
    } else if (mimeType.contains("json")) {
      JsonElement(contentStr, null, uri) // not serialize, parsing is faster
    } else if (mimeType.contains("csv")) {
      val csvFormat: CSVFormat = this.metadata.asMap
        .get(Doc.CSV_FORMAT)
        .map {
          case v: CSVFormat => v
          case v @ _        => CSVFormat.valueOf(v.toString)
        }
        .getOrElse(Doc.defaultCSVFormat)

      CSVElement.Block.apply(contentStr, uri, csvFormat) // not serialize, parsing is faster
    } else if (mimeType.contains("plain") || mimeType.contains("text")) {
      PlainElement(contentStr, uri) // not serialize, parsing is faster
    } else {
      HtmlElement.fromBytes(raw, effectiveCharset, mimeType, uri)
    }
  }
  def charset: Option[String] = Option(parsedContentType.getCharset).map(_.name())
  def mimeType: String = parsedContentType.getMimeType

  def isImage: Boolean = mimeType.startsWith("image")

  def contentType: String = parsedContentType.toString

  lazy val tikaMimeType: MimeType = MimeTypes.getDefaultMimeTypes.forName(mimeType)
  lazy val fileExtensions: Seq[String] = tikaMimeType.getExtensions.asScala.toSeq.map { str =>
    if (str.startsWith(".")) str.splitAt(1)._2
    else str
  }
  def defaultFileExtension: Option[String] = fileExtensions.headOption

  // ---------------------------------------------------------------------------------------------------

  def save(
      pathParts: Seq[String],
      overwrite: Boolean = false
  )(spooky: SpookyContext): Unit = {

    val path = CommonUtils.\\\(pathParts: _*)

    DocUtils.dfsWrite("save", path, spooky) { progress =>
      val fullPath = new Path(path)
      val fs = fullPath.getFileSystem(spooky.hadoopConf)
      //      if (!overwrite && fs.exists(fullPath)) fullPath = new Path(path + "-" + UUID.randomUUID())
      val fos =
        try {
          fs.create(fullPath, overwrite)
        } catch {
          case _: Exception =>
            val altPath = new Path(path + "-" + UUID.randomUUID())
            fs.create(altPath, overwrite)
        }

      val os = progress.WrapOStream(fos)

      try {
        os.write(raw) //       remember that apache IOUtils is defective for DFS!

        val metrics = spooky.spookyMetrics
        metrics.saved += 1
//        metrics.savedPath.add(path -> progress.indicator.longValue())

        saved += fullPath.toString
      } finally {
        os.close()
      }
    }
  }

  def autoSave(
      spooky: SpookyContext,
      overwrite: Boolean = false
  ): Unit =
    this.save(
      spooky.dirConf.autoSave :: spooky.spookyConf.autoSaveFilePath(this) :: Nil,
      overwrite
    )(spooky)

  // TODO: merge into cascade retries
  def errorDump(
      spooky: SpookyContext,
      overwrite: Boolean = false
  ): Unit = {
    val root =
      if (this.isImage) spooky.dirConf.errorScreenshot
      else spooky.dirConf.errorDump

    this.save(
      root :: spooky.spookyConf.errorDumpFilePath(this) :: Nil,
      overwrite
    )(spooky)
  }

  def errorDumpLocally(
      spooky: SpookyContext,
      overwrite: Boolean = false
  ): Unit = {
    val root =
      if (this.isImage) spooky.dirConf.errorScreenshot
      else spooky.dirConf.errorDump

    this.save(
      root :: spooky.spookyConf.errorDumpFilePath(this) :: Nil,
      overwrite
    )(spooky)
  }

  def setMetadata(tuples: (String, Any)*): Doc = {

    this.copy(
      metadata = this.metadata ++: ResourceMetadata.From.tuple(tuples: _*)
    )
  }
}
