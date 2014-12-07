package org.tribbloid.spookystuff.pages

import java.io._
import java.util.{Date, UUID}

import org.apache.hadoop.fs.Path
import org.apache.http.entity.ContentType
import org.tribbloid.spookystuff._
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.utils.Utils

/**
 * Created by peng on 04/06/14.
 */
//use to genterate a lookup key for each page so
@SerialVersionUID(612503421395L)
case class PageUID(
                    backtrace: Trace,
                    leaf: Named,
                    blockKey: Int = -1 //-1 is no sub key
                    )

trait PageLike {
  val uid: PageUID
}

//Merely a placeholder when a Block returns nothing
case class NoPage(override val uid: PageUID) extends Serializable with PageLike

//immutable! we don't want to lose old pages
//keep small, will be passed around by Spark
@SerialVersionUID(94865098324L)
case class Page(
            override val uid: PageUID,

            override val uri: String, //redirected
            contentType: String,
            content: Array[Byte],

            //                 cookie: Seq[SerializableCookie] = Seq(),
            timestamp: Date = new Date,
            var saved: String = null
            )
  extends Unstructured with PageLike {

  def name = this.uid.leaf.name

  //TODO: these customization is to ensure that little resources is spent on comparison
//  override def equals(obj: Any): Boolean = obj match {
//    case other: Page =>
//      (this.uid == other.uid) && (this.name == other.name) && (this.timestamp == other.timestamp)
//    case _ => false
//  }
//
//  override def hashCode(): Int = (this.uid, this.name, this.timestamp).hashCode()

  @transient lazy val parsedContentType: ContentType = {
    var result = ContentType.parse(this.contentType)
    if (result.getCharset == null) result = result.withCharset(Const.defaultCharset)
    result
  }

  @transient lazy val root: Unstructured = if (parsedContentType.getMimeType.contains("html")) {
    new HtmlElement(content, parsedContentType.getCharset, uri) //not serialize, parsing is faster
  }
  else {
    null
//    throw new UnsupportedOperationException("Cannot parse mime type " + parsedContentType.getMimeType)
  }

  override def children(selector: String): Seq[Unstructured] = root.children(selector)
  override def markup: Option[String] = root.markup
  override def attr(attr: String, noEmpty: Boolean): Option[String] = root.attr(attr, noEmpty)
  override def text: Option[String] = root.text
  override def ownText: Option[String] = root.ownText
  override def boilerPipe(): Option[String] = root.boilerPipe()
  //---------------------------------------------------------------------------------------------------

  //this will lose information as charset encoding will be different
  def save(
            pathParts: Seq[String],
            overwrite: Boolean = false
            )(spooky: SpookyContext): Page = {

    val path = Utils.urlConcat(pathParts: _*)

    PageUtils.DFSWrite("save", path, spooky) {

      var fullPath = new Path(path)

      val fs = fullPath.getFileSystem(spooky.hConf)

      if (!overwrite && fs.exists(fullPath)) fullPath = new Path(path + "-" + UUID.randomUUID())

      val fos = fs.create(fullPath, overwrite)

      try {
        fos.write(content) //       remember that apache IOUtils is defective for DFS!
      }
      finally {
        fos.close()
      }

      this.saved = fullPath.toString
      this
    }
  }

  def autoSave(
                spooky: SpookyContext,
                overwrite: Boolean = false
                ): Page = this.save(
    spooky.autoSaveRoot :: spooky.autoSaveExtract(this).toString :: Nil
  )(spooky)

  def errorDump(
                 spooky: SpookyContext,
                 overwrite: Boolean = false
                 ): Page = {
    val root = this.uid.leaf match {
      case ss: Screenshot => spooky.errorDumpScreenshotRoot
      case _ => spooky.errorDumpRoot
    }

    this.save(
      root :: spooky.errorDumpExtract(this).toString :: Nil
    )(spooky)
  }

  def localErrorDump(
                      spooky: SpookyContext,
                      overwrite: Boolean = false
                      ): Page = {
    val root = this.uid.leaf match {
      case ss: Screenshot => spooky.localErrorDumpScreenshotRoot
      case _ => spooky.localErrorDumpRoot
    }

    this.save(
      root :: spooky.errorDumpExtract(this).toString :: Nil
    )(spooky)
  }
}