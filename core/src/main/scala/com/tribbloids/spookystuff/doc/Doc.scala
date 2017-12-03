package com.tribbloids.spookystuff.doc

import java.sql.{Date, Time, Timestamp}
import java.util.UUID

import com.tribbloids.spookystuff._
import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.caching.CacheLevel
import com.tribbloids.spookystuff.utils.refl.ScalaUDT
import com.tribbloids.spookystuff.utils.{CommonUtils, IDMixin}
import org.apache.commons.csv.CSVFormat
import org.apache.hadoop.fs.Path
import org.apache.http.StatusLine
import org.apache.http.entity.ContentType
import org.apache.spark.sql.types.SQLUserDefinedType
import org.apache.tika.io.TikaInputStream
import org.apache.tika.metadata.{Metadata, TikaMetadataKeys}
import org.apache.tika.mime.MimeTypes
import org.mozilla.universalchardet.UniversalDetector

/**
  * Created by peng on 04/06/14.
  */
//use to genterate a lookup key for each page so
@SerialVersionUID(612503421395L)
case class DocUID(
                   backtrace: Trace,
                   output: Export,
                   //                    sessionStartTime: Long,
                   blockIndex: Int = 0,
                   blockSize: Int = 1
                 )(//number of pages in a block output,
                   val name: String = Option(output).map(_.name).orNull
                 ) {

}

class FetchedUDT extends ScalaUDT[Fetched]

//keep small, will be passed around by Spark
//TODO: subclass Unstructured to save Message definition
@SQLUserDefinedType(udt = classOf[FetchedUDT])
trait Fetched extends Serializable {

  def uid: DocUID
  def updated(
               uid: DocUID = this.uid,
               cacheLevel: CacheLevel.Value = this.cacheLevel
             ): this.type

  def cacheLevel: CacheLevel.Value

  def name: String = this.uid.name

  def timeMillis: Long

  lazy val date: Date = new Date(timeMillis)
  lazy val time: Time = new Time(timeMillis)
  lazy val timestamp: Timestamp = new Timestamp(timeMillis)

  def laterThan(v2: Fetched): Boolean = this.timeMillis > v2.timeMillis

  def laterOf(v2: Fetched): Fetched = if (laterThan(v2)) this
  else v2

  type RootType
  def root: RootType
  def metadata: Map[String, Any]
}

//Merely a placeholder if a conditional block is not applicable
case class NoDoc(
                  backtrace: Trace,
                  override val timeMillis: Long = System.currentTimeMillis(),
                  override val cacheLevel: CacheLevel.Value = CacheLevel.All,
                  metadata: Map[String, Any] = Map.empty
                ) extends Serializable with Fetched {

  @transient override lazy val uid: DocUID = DocUID(backtrace, null, 0, 1)()

  override def updated(
                        uid: DocUID = this.uid,
                        cacheLevel: CacheLevel.Value = this.cacheLevel
                      ) = this.copy(backtrace = uid.backtrace, cacheLevel = cacheLevel).asInstanceOf[this.type ]

  override type RootType = Unit
  override def root: Unit = {}
}

case class DocWithError(
                         delegate: Doc,
                         header: String = "",
                         override val cause: Throwable = null
                       ) extends ActionException(

  header + delegate.root.formattedCode.map(
    "\n" + _
  )
    .getOrElse(""),
  cause
) with  Fetched {

  override def timeMillis: Long = delegate.timeMillis

  override def uid: DocUID = delegate.uid

  override def updated(
                        uid: DocUID = this.uid,
                        cacheLevel: CacheLevel.Value = this.cacheLevel
                      ) = {
    this.copy(delegate = delegate.updated(uid, cacheLevel)).asInstanceOf[this.type]
  }

  override def cacheLevel: CacheLevel.Value = delegate.cacheLevel

  override type RootType = delegate.RootType
  override def root: Unstructured = delegate.root

  override def metadata: Map[String, Any] = delegate.metadata
}

object Doc {

  val CONTENT_TYPE = "contentType"
  val CSV_FORMAT = "csvFormat"

  val defaultCSVFormat = CSVFormat.DEFAULT
}


@SerialVersionUID(94865098324L)
@SQLUserDefinedType(udt = classOf[UnstructuredUDT])
case class Doc(
                override val uid: DocUID,

                uri: String, //redirected
                declaredContentType: Option[String],
                raw: Array[Byte],
                //                 cookie: Seq[SerializableCookie] = Nil,
                override val timeMillis: Long = System.currentTimeMillis(),
                saved: scala.collection.mutable.Set[String] = scala.collection.mutable.Set(),
                override val cacheLevel: CacheLevel.Value = CacheLevel.All,
                httpStatus: Option[StatusLine] = None,
                metadata: Map[String, Any] = Map.empty //for customizing parsing TODO: remove, delegate to CSVElement.
              ) extends Fetched with IDMixin {

  lazy val _id = (uid, uri, declaredContentType, timeMillis, httpStatus.toString)

  override def updated(
                        uid: DocUID = this.uid,
                        cacheLevel: CacheLevel.Value = this.cacheLevel
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
    }
    else detected
  }

  @transient lazy val parsedContentType: ContentType = {
    val strOpt = metadata
      .get(Doc.CONTENT_TYPE)
      .map("" + _)
      .orElse(declaredContentType)
    strOpt match {
      case Some(str) =>
        val ct = ContentType.parse(str)
        if (ct.getCharset == null) {

          ct.withCharset(detectCharset(ct))
        }
        else ct
      case None =>
        val metadata = new Metadata()
        val slash: Int = uri.lastIndexOf('/')
        metadata.set(TikaMetadataKeys.RESOURCE_NAME_KEY, uri.substring(slash + 1))
        val stream = TikaInputStream.get(raw, metadata)
        try {
          val mediaType = Const.mimeDetector.detect(stream, metadata)
          //        val mimeType = mediaType.getBaseType.toString
          //        val charset = new CharsetDetector().getString(content, null)
          //        ContentType.create(mimeType, charset)

          val str = mediaType.toString
          val result = ContentType.parse(str)
          if (result.getCharset == null) {

            result.withCharset(detectCharset(result))
          }
          else result
        }
        finally {
          stream.close()
        }
    }
  }

  override type RootType = Unstructured
  //TODO: use reflection to find any element implementation that can resolve supplied MIME type
  @transient override lazy val root: Unstructured = {
    val effectiveCharset = charset.orNull

    val contentStr = new String(raw, effectiveCharset)
    if (mimeType.contains("html") || mimeType.contains("xml") || mimeType.contains("directory")) {
      HtmlElement(contentStr, uri) //not serialize, parsing is faster
    }
    else if (mimeType.contains("json")) {
      JsonElement(contentStr, null, uri) //not serialize, parsing is faster
    }
    else if (mimeType.contains("csv")) {
      val csvFormat = this.metadata.get(Doc.CSV_FORMAT).map{
        _.asInstanceOf[CSVFormat]
      }
        .getOrElse(Doc.defaultCSVFormat)

      CSVElement.apply(contentStr, uri, csvFormat) //not serialize, parsing is faster
    }
    else if (mimeType.contains("plain")) {
      PlainElement(contentStr, uri) //not serialize, parsing is faster
    }
    else {
      TikaMetadataXMLElement(raw, effectiveCharset, mimeType, uri)
    }
  }
  def charset: Option[Selector] = Option(parsedContentType.getCharset).map(_.name())
  def mimeType: String = parsedContentType.getMimeType

  def contentType = parsedContentType.toString

  def tikaMimeType = MimeTypes.getDefaultMimeTypes.forName(mimeType)
  def fileExtensions: Array[String] = tikaMimeType.getExtensions.toArray(Array[String]()).map{
    str =>
      if (str.startsWith(".")) str.splitAt(1)._2
      else str
  }
  def defaultFileExtension: Option[String] = fileExtensions.headOption

//  override def findAll(selector: String) = root.findAll(selector)
//  override def findAllWithSiblings(start: String, range: Range) = root.findAllWithSiblings(start, range)
//  override def children(selector: Selector): Elements[Unstructured] = root.children(selector)
//  override def childrenWithSiblings(selector: Selector, range: Range): Elements[Siblings[Unstructured]] = root.childrenWithSiblings(selector, range)
//  override def code: Option[String] = root.code
//  override def formattedCode: Option[String] = root.formattedCode
//  override def allAttr: Option[Map[String, String]] = root.allAttr
//  override def attr(attr: String, noEmpty: Boolean): Option[String] = root.attr(attr, noEmpty)
//  override def href: Option[String] = root.href
//  override def src: Option[String] = root.src
//  override def text: Option[String] = root.text
//  override def ownText: Option[String] = root.ownText
//  override def boilerPipe: Option[String] = root.boilerPipe
//  override def breadcrumb: Option[Seq[String]] = root.breadcrumb TODO: remove
  //---------------------------------------------------------------------------------------------------

  def save(
            pathParts: Seq[String],
            overwrite: Boolean = false
          )(spooky: SpookyContext): Unit = {

    val path = CommonUtils.\\\(pathParts: _*)

    DocUtils.dfsWrite("save", path, spooky) {

      val fullPath = new Path(path)
      val fs = fullPath.getFileSystem(spooky.hadoopConf)
      //      if (!overwrite && fs.exists(fullPath)) fullPath = new Path(path + "-" + UUID.randomUUID())
      val fos = try {
        fs.create(fullPath, overwrite)
      }
      catch {
        case e: Throwable =>
          val altPath = new Path(path + "-" + UUID.randomUUID())
          fs.create(altPath, overwrite)
      }

      try {
        fos.write(raw) //       remember that apache IOUtils is defective for DFS!
      }
      finally {
        fos.close()
      }

      saved += fullPath.toString
    }
  }

  def autoSave(
                spooky: SpookyContext,
                overwrite: Boolean = false
              ): Unit = this.save(
    spooky.dirConf.autoSave :: spooky.spookyConf.autoSaveFilePath(this).toString :: Nil
  )(spooky)

  //TODO: merge into cascade retries
  def errorDump(
                 spooky: SpookyContext,
                 overwrite: Boolean = false
               ): Unit = {
    val root = this.uid.output match {
      case ss: Screenshot => spooky.dirConf.errorScreenshot
      case _ => spooky.dirConf.errorDump
    }

    this.save(
      root :: spooky.spookyConf.errorDumpFilePath(this).toString :: Nil
    )(spooky)
  }

  def errorDumpLocally(
                        spooky: SpookyContext,
                        overwrite: Boolean = false
                      ): Unit = {
    val root = this.uid.output match {
      case ss: Screenshot => spooky.dirConf.errorScreenshotLocal
      case _ => spooky.dirConf.errorDumpLocal
    }

    this.save(
      root :: spooky.spookyConf.errorDumpFilePath(this).toString :: Nil
    )(spooky)
  }

  def set(tuples: (String, Any)*): Doc = this.copy(
    metadata = this.metadata ++ Map(tuples: _*)
  )
}