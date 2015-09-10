package com.tribbloids.spookystuff.pages

import java.io._
import java.util.Date

import org.apache.tika.mime.MimeTypes

//TODO: change to sql.Date
import java.util.UUID

import com.tribbloids.spookystuff._
import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.utils.Utils
import org.apache.hadoop.fs.Path
import org.apache.http.entity.ContentType
import org.apache.tika.io.TikaInputStream
import org.apache.tika.metadata.{Metadata, TikaMetadataKeys}
import org.mozilla.universalchardet.UniversalDetector

import scala.collection.immutable.ListSet

/**
 * Created by peng on 04/06/14.
 */
//use to genterate a lookup key for each page so
@SerialVersionUID(612503421395L)
case class PageUID(
                    backtrace: Trace,
                    output: Export,
                    //                    sessionStartTime: Long,
                    blockIndex: Int = 0,
                    blockSize: Int = 1 //number of pages in a block output,
                    ) {

}

trait PageLike {
  val uid: PageUID
  val timestamp: Date
  val cacheable: Boolean

  def laterThan(v2: PageLike): Boolean = this.timestamp after v2.timestamp

  def laterOf(v2: PageLike): PageLike = if (laterThan(v2)) this
  else v2
}

//Merely a placeholder when a Block returns nothing
case class NoPage(
                   trace: Trace,
                   override val timestamp: Date = new Date(System.currentTimeMillis()),
                   override val cacheable: Boolean = true
                   ) extends Serializable with PageLike {

  override val uid: PageUID = PageUID(trace, null, 0, 1)
}

//keep small, will be passed around by Spark
@SerialVersionUID(94865098324L)
case class Page(
                 override val uid: PageUID,

                 override val uri: String, //redirected
                 declaredContentType: Option[String],
                 content: Array[Byte],

                 //                 cookie: Seq[SerializableCookie] = Nil,
                 override val timestamp: Date = new Date(System.currentTimeMillis()),
                 var saved: ListSet[String] = ListSet(),
                 override val cacheable: Boolean = true
                 )
  extends Unstructured with PageLike {

  def name = this.uid.output.name

  private def detectCharset(result: ContentType): String = {
    val charsetD = new UniversalDetector(null)
    val ss = 4096

    for (i <- 0.until(content.length, ss)) {
      val length = Math.min(content.length - i, ss)
      charsetD.handleData(content, i, length)
      if (charsetD.isDone) return charsetD.getDetectedCharset
    }

    charsetD.dataEnd()
    val detected = charsetD.getDetectedCharset

    if (detected == null) {
      if (result.getMimeType.contains("text")) Const.defaultTextCharset
      else if (result.getMimeType.contains("application")) Const.defaultApplicationCharset
      else Const.defaultApplicationCharset
    }
    else detected
  }

  @transient lazy val parsedContentType: ContentType = declaredContentType match {
    case Some(str) =>
      val result = ContentType.parse(str)
      if (result.getCharset == null) {


        result.withCharset(detectCharset(result))
      }
      else result
    case None =>
      val metadata = new Metadata()
      val slash: Int = uri.lastIndexOf('/')
      metadata.set(TikaMetadataKeys.RESOURCE_NAME_KEY, uri.substring(slash + 1))
      val stream = TikaInputStream.get(content, metadata)
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

  def contentType = parsedContentType.toString

  def mimeType: String = parsedContentType.getMimeType
  def charset: Option[Selector] = Option(parsedContentType.getCharset).map(_.name())
  def tikaMime = MimeTypes.getDefaultMimeTypes.forName(mimeType)
  def exts: Array[String] = tikaMime.getExtensions.toArray(Array[String]()).map{
    str =>
      if (str.startsWith(".")) str.splitAt(1)._2
      else str
  }
  def defaultExt: Option[String] = exts.headOption

  //TODO: use reflection to find any element implementation that can resolve supplied MIME type
  @transient lazy val root: Unstructured = {
    val effectiveCharset = charset.orNull

    if (mimeType.contains("html")) {
      HtmlElement(content, effectiveCharset, uri) //not serialize, parsing is faster
    }
    else if (mimeType.contains("xml")) {
      HtmlElement(content, effectiveCharset, uri) //not serialize, parsing is faster
    }
    else if (mimeType.contains("json")) {
      JsonElement(content, effectiveCharset, uri) //not serialize, parsing is faster
    }
    else {
      TikaHtmlElement(content, effectiveCharset, mimeType, uri)
    }
  }

  override def findAll(selector: String) = root.findAll(selector)
  override def findAllWithSiblings(start: String, range: Range) = root.findAllWithSiblings(start, range)
  override def children(selector: Selector): Elements[Unstructured] = root.children(selector)
  override def childrenWithSiblings(selector: Selector, range: Range): Elements[Siblings[Unstructured]] = root.childrenWithSiblings(selector, range)
  override def code: Option[String] = root.code
  override def formattedCode: Option[String] = root.formattedCode
  override def attr(attr: String, noEmpty: Boolean): Option[String] = root.attr(attr, noEmpty)
  override def href: Option[String] = root.href
  override def src: Option[String] = root.src
  override def text: Option[String] = root.text
  override def ownText: Option[String] = root.ownText
  override def boilerPipe: Option[String] = root.boilerPipe
  //---------------------------------------------------------------------------------------------------

  //this will lose information as charset encoding will be different
  def save(
            pathParts: Seq[String],
            overwrite: Boolean = false
            )(spooky: SpookyContext): Unit = {

    val path = Utils.uriConcat(pathParts: _*)

    PageUtils.dfsWrite("save", path, spooky) {

      var fullPath = new Path(path)
      val fs = fullPath.getFileSystem(spooky.hadoopConf)
      if (!overwrite && fs.exists(fullPath)) fullPath = new Path(path + "-" + UUID.randomUUID())
      val fos = fs.create(fullPath, overwrite)
      try {
        fos.write(content) //       remember that apache IOUtils is defective for DFS!
      }
      finally {
        fos.close()
      }

      saved = saved + fullPath.toString
    }
  }

  def autoSave(
                spooky: SpookyContext,
                overwrite: Boolean = false
                ): Unit = this.save(
    spooky.conf.dirs.autoSave :: spooky.conf.autoSavePath(this).toString :: Nil
  )(spooky)

  def errorDump(
                 spooky: SpookyContext,
                 overwrite: Boolean = false
                 ): Unit = {
    val root = this.uid.output match {
      case ss: Screenshot => spooky.conf.dirs.errorScreenshot
      case _ => spooky.conf.dirs.errorDump
    }

    this.save(
      root :: spooky.conf.errorDumpPath(this).toString :: Nil
    )(spooky)
  }

  def errorDumpLocally(
                        spooky: SpookyContext,
                        overwrite: Boolean = false
                        ): Unit = {
    val root = this.uid.output match {
      case ss: Screenshot => spooky.conf.dirs.errorScreenshotLocal
      case _ => spooky.conf.dirs.errorDumpLocal
    }

    this.save(
      root :: spooky.conf.errorDumpPath(this).toString :: Nil
    )(spooky)
  }
}