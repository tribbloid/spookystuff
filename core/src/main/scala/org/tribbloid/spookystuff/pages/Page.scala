package org.tribbloid.spookystuff.pages

import java.io._
import java.util.{Date, UUID}

import org.apache.hadoop.fs.Path
import org.apache.http.entity.ContentType
import org.tribbloid.spookystuff._
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.utils.Utils

import scala.collection.immutable.ListSet

/**
 * Created by peng on 04/06/14.
 */
//use to genterate a lookup key for each page so
@SerialVersionUID(612503421395L)
case class PageUID(
                    backtrace: Trace,
                    output: Named,
                    //                    sessionStartTime: Long, //TODO: add for sanity check
                    blockIndex: Int = 0,
                    blockSize: Int = 1 //number of pages in a block output,
                    ) {

}

trait PageLike {
  val uid: PageUID
  val timestamp: Date

  def laterThan(v2: PageLike): Boolean = this.timestamp after v2.timestamp

  def laterOf(v2: PageLike): PageLike = if (laterThan(v2)) this
  else v2
}

//Merely a placeholder when a Block returns nothing
case class NoPage(
                   trace: Trace,
                   override val timestamp: Date = new Date
                   ) extends Serializable with PageLike {

  override val uid: PageUID = PageUID(trace, null, 0, 1)
}

//keep small, will be passed around by Spark
@SerialVersionUID(94865098324L)
case class Page(
                 override val uid: PageUID,

                 override val uri: String, //redirected
                 contentType: String,
                 content: Array[Byte],

                 //                 cookie: Seq[SerializableCookie] = Seq(),
                 override val timestamp: Date = new Date,
                 var saved: ListSet[String] = ListSet()
                 )
  extends Unstructured with PageLike {

  def name = this.uid.output.name

  @transient lazy val parsedContentType: ContentType = {
    var result = ContentType.parse(this.contentType)
    if (result.getCharset == null) result = result.withCharset(Const.defaultCharset)
    result
  }

  //TODO: use reflection to find any element implementation that can resolve supplied MIME type
  @transient lazy val root: Unstructured =
    if (parsedContentType.getMimeType.contains("html")) {
      new HtmlElement(content, parsedContentType.getCharset, uri) //not serialize, parsing is faster
    }
    else if (parsedContentType.getMimeType.contains("xml")) {
      new HtmlElement(content, parsedContentType.getCharset, uri) //not serialize, parsing is faster
    }
    else {
      new UnknownElement(uri)
    }

  override def children(selector: String) = root.children(selector)
  override def childrenWithSiblings(start: String, range: Range) = root.childrenWithSiblings(start, range)
  override def code: Option[String] = root.code
  override def attr(attr: String, noEmpty: Boolean): Option[String] = root.attr(attr, noEmpty)
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

    PageUtils.DFSWrite("save", path, spooky) {

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

  def errorDumpLocal(
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