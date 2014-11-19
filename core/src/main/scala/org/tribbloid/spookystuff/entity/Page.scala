package org.tribbloid.spookystuff.entity

import java.io._
import java.util.{Date, UUID}

import de.l3s.boilerpipe.extractors.ArticleExtractor
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.Path
import org.apache.http.entity.ContentType
import org.apache.spark.SparkEnv
import org.jsoup.Jsoup
import org.jsoup.nodes.{Document, Element}
import org.slf4j.LoggerFactory
import org.tribbloid.spookystuff._
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.utils.{Cacheable, Const, Utils}

import scala.collection.JavaConversions._

object Page {

  def DFSRead[T](message: String, pathStr: String, spooky: SpookyContext)(f: => T): T = {
    try {
      val result = Utils.retry(Const.DFSInPartitionRetry) {
        Utils.withDeadline(spooky.DFSTimeout) {f}
      }
      spooky.metrics.DFSReadSuccess += 1
      result
    }
    catch {
      case e: Throwable =>
        spooky.metrics.DFSReadFail += 1
        val ex = new DFSReadException(pathStr ,e)
        ex.setStackTrace(e.getStackTrace)
        if (spooky.failOnDFSError) throw ex
        else {
          LoggerFactory.getLogger(this.getClass).warn(message, ex)
          null.asInstanceOf[T]
        }
    }
  }

  //always fail on retry depletion and timeout
  def DFSWrite[T](message: String, pathStr: String, spooky: SpookyContext)(f: => T): T = {
    try {
      Utils.retry(Const.DFSInPartitionRetry) {
        Utils.withDeadline(spooky.DFSTimeout) {f}
      }
    }
    catch {
      case e: Throwable =>
        val ex = new DFSWriteException(pathStr ,e)
        ex.setStackTrace(e.getStackTrace)
        throw ex
    }
  }

  def load(fullPath: Path)(spooky: SpookyContext): Array[Byte] = {

    DFSRead("load", fullPath.toString, spooky) {
      val fs = fullPath.getFileSystem(spooky.hConf)

      if (fs.exists(fullPath)) {

        val fis = fs.open(fullPath)

        try {
          IOUtils.toByteArray(fis)
        }
        finally {
          fis.close()
        }
      }
      else null
    }
  }

  //unlike save, this will store all information in an unreadable, serialized, probably compressed file
  //always overwrite
  def cache(
             pages: Seq[Page],
             path: String,
             overwrite: Boolean = false
             )(spooky: SpookyContext): Unit = {

    DFSWrite("cache", path, spooky) {
      val fullPath = new Path(path)

      val fs = fullPath.getFileSystem(spooky.hConf)

      val ser = SparkEnv.get.serializer.newInstance()
      val fos = fs.create(fullPath, overwrite)
      val serOut = ser.serializeStream(fos)

      try {
        serOut.writeObject[Seq[Page]](Cacheable[Seq[Page]](pages))
      }
      finally {
        fos.close()
        serOut.close()
      }
    }
  }

  def autoCache(
                 pages: Seq[Page],
                 trace: Trace,
                 spooky: SpookyContext
                 ): Unit = {
    val pathStr = Utils.urlConcat(
      spooky.autoCacheRoot,
      spooky.cacheTraceEncoder(trace).toString,
      UUID.randomUUID().toString
    )

    Page.cache(pages, pathStr)(spooky)
  }

  def restore(fullPath: Path)(spooky: SpookyContext): Seq[Page] = {

    DFSRead("restore", fullPath.toString, spooky) {
      val fs = fullPath.getFileSystem(spooky.hConf)

      if (fs.exists(fullPath)) {

        val ser = SparkEnv.get.serializer.newInstance()
        val fis = fs.open(fullPath)
        val serIn = ser.deserializeStream(fis)
        try {
          val obj = serIn.readObject[Seq[Page]]()
          obj.asInstanceOf[Seq[Page]]
        }
        finally{
          fis.close()
          serIn.close()
        }
      }
      else null
    }
  }

  //  class PrefixFilter(val prefix: String) extends PathFilter {
  //
  //    override def accept(path: Path): Boolean = path.getName.startsWith(prefix)
  //  }
  //
  //  def getDirsByPrefix(dirPath: Path, prefix: String)(hConf: Configuration): Seq[Path] = {
  //
  //    val fs = dirPath.getFileSystem(hConf)
  //
  //    if (fs.getFileStatus(dirPath).isDir) {
  //      val status = fs.listStatus(dirPath, new PrefixFilter(prefix))
  //
  //      status.map(_.getPath)
  //    }
  //    else Seq()
  //  }

  //restore latest in a directory
  //returns: Seq() => has backtrace dir but contains no page
  //returns null => no backtrace dir
  def restoreLatest(
                     dirPath: Path,
                     earliestModificationTime: Long = 0
                     )(spooky: SpookyContext): Seq[Page] = {

    val latestStatus = DFSRead("get latest version", dirPath.toString, spooky) {

      val fs = dirPath.getFileSystem(spooky.hConf)

      if (fs.exists(dirPath) && fs.getFileStatus(dirPath).isDir) {

        val statuses = fs.listStatus(dirPath)

        statuses.filter(status => !status.isDir && status.getModificationTime >= earliestModificationTime)
          .sortBy(_.getModificationTime).lastOption
      }
      else None
    }

    latestStatus match {
      case Some(status) => restore(status.getPath)(spooky)
      case _ => null
    }
  }

  //TODO: cannot handle infinite duration, avoid using it!
  def autoRestoreLatest(
                         trace: Trace,
                         spooky: SpookyContext
                         ): Seq[Page] = {
    val pathStr = Utils.urlConcat(
      spooky.autoCacheRoot,
      spooky.cacheTraceEncoder(trace).toString
    )

    val pages = restoreLatest(
      new Path(pathStr),
      System.currentTimeMillis() - spooky.pageExpireAfter.toMillis
    )(spooky)

    if (pages != null) for (page <- pages) {
      val pageTrace: Trace = page.uid.backtrace

      pageTrace.inject(trace.asInstanceOf[pageTrace.type ])
      //this is to allow actions in backtrace to have different name than those cached
    }
    pages
  }
}

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

//immutable! we don't want to lose old pages
//keep small, will be passed around by Spark
@SerialVersionUID(94865098324L)
case class Page(
                 uid: PageUID,

                 url: String, //redirected
                 contentType: String,
                 content: Array[Byte],

                 //                 cookie: Seq[SerializableCookie] = Seq(),
                 timestamp: Date = new Date,
                 saved: String = null
                 )
  extends Serializable {

  @transient lazy val parsedContentType: ContentType = {
    var result = ContentType.parse(this.contentType)
    if (result.getCharset == null) result = result.withCharset(Const.defaultCharset)
    result
  }
  @transient lazy val contentStr: String = new String(this.content,this.parsedContentType.getCharset)

  @transient lazy val doc: Option[Any] = if (parsedContentType.getMimeType.contains("html")){
    Some(Jsoup.parse(this.contentStr, url)) //not serialize, parsing is faster
  }
  else{
    None
  }

  def backtrace = this.uid.backtrace
  def blockKey = this.uid.blockKey

  def name = this.uid.leaf.name

  //this will lose information as charset encoding will be different
  def save(
            pathParts: Seq[String],
            overwrite: Boolean = false
            //            metadata: Boolean = true
            )(spooky: SpookyContext): Page = {

    val path = Utils.urlConcat(pathParts: _*)

    Page.DFSWrite("save", path, spooky) {

      var fullPath = new Path(path)

      val fs = fullPath.getFileSystem(spooky.hConf)

      if (!overwrite && fs.exists(fullPath)) fullPath = new Path(path +"-"+ UUID.randomUUID())

      val fos = fs.create(fullPath, overwrite)

      try {
        fos.write(content)//       remember that apache IOUtils is defective for DFS!
      }
      finally {
        fos.close()
      }

      this.copy(saved = fullPath.toString)
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
    val root = this.uid.leaf match{
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
    val root = this.uid.leaf match{
      case ss: Screenshot => spooky.localErrorDumpScreenshotRoot
      case _ => spooky.localErrorDumpRoot
    }

    this.save(
      root :: spooky.errorDumpExtract(this).toString :: Nil
    )(spooky)
  }

  def numElements(selector: String): Int = doc match {
    case Some(doc: Element) => doc.select(selector).size()

    case _ => 0
  }

  def elementExist(selector: String): Boolean = numElements(selector) > 0

  def attrExist(
                 selector: String,
                 attr: String
                 ): Boolean = {

    elementExist(selector) && (doc match {

      case Some(doc: Element) => doc.select(selector).hasAttr(attr)

      case _ => false
    })
  }

  /**
   * Return attribute of an element.
   * return null if selector has no match, return "" if it has a match but attribute doesn't exist
   * @param selector css selector of the element, only the first match will be return
   * @param attr attribute
   * @return value of the attribute as string
   */
  def attr1(
             selector: String,
             attr: String,
             noEmpty: Boolean = true,
             last: Boolean = false
             ): String = {
    if (!last) this.attr(selector, attr, noEmpty).headOption.orNull
    else this.attr(selector, attr, noEmpty).lastOption.orNull
  }

  /**
   * Return a sequence of attributes of all elements that match the selector.
   * return [] if selector has no match,
   * returned Sequence may contains "" for elements that match the selector but without required attribute, use filter if you don't want them
   * @param selector css selector of all elements
   * @param attr attribute
   * @return values of the attributes as a sequence of strings
   */
  def attr(
            selector: String,
            attr: String,
            noEmpty: Boolean = true
            ): Seq[String] = doc match {
    case Some(doc: Element) =>

      val elements = doc.select(selector)

      val result = elements.map {
        _.attr(attr)
      }

      if (noEmpty) result.filter(_.nonEmpty)
      else result

    case _ => Seq[String]()
  }

  /**
   * Shorthand for attr1("href")
   * @param selector css selector of the element
   * @param absolute whether to use absolute path (site url + relative path) or relative path, default to true
   * @return value of the attribute as string
   */
  def href1(
             selector: String,
             absolute: Boolean = true,
             noEmpty: Boolean = true
             ): String = this.href(selector, absolute, noEmpty).headOption.orNull

  /**
   * Shorthand for attr("href")
   * @param selector css selector of all elements
   * @param absolute whether to use absolute path (site url + relative path) or relative path, default to true
   * @return values of the attributes as a sequence of strings
   */
  def href(
            selector: String,
            absolute: Boolean = true,
            noEmpty: Boolean = true
            ): Seq[String] = {
    if (absolute) attr(selector,"abs:href")
    else attr(selector,"href")
  }

  /**
   * Shorthand for attr1("src")
   * @param selector css selector of the element
   * @param absolute whether to use absolute path (site url + relative path) or relative path, default to true
   * @return value of the attribute as string
   */
  def src1(
            selector: String,
            absolute: Boolean = true,
            noEmpty: Boolean = true
            ): String = this.src(selector, absolute, noEmpty).headOption.orNull

  /**
   * Shorthand for attr("src")
   * @param selector css selector of all elements
   * @param absolute whether to use absolute path (site url + relative path) or relative path, default to true
   * @return values of the attributes as a sequence of strings
   */
  def src(
           selector: String,
           absolute: Boolean = true,
           noEmpty: Boolean = true
           ): Seq[String] = {
    if (absolute) attr(selector,"abs:src",noEmpty)
    else attr(selector,"src",noEmpty)
  }

  //return null if selector found nothing, return "" if found something without text
  /**
   * Return all text enclosed by an element.
   * return null if selector has no match
   * @param selector css selector of the element, only the first match will be return
   * @return enclosed text as string
   */
  def text1(
             selector: String,
             own: Boolean = false,
             last: Boolean = false
             ): String = {
    if (!last) this.text(selector, own).headOption.orNull
    else this.text(selector, own).lastOption.orNull
  }

  /** Return an seq of texts enclosed by their respective elements
    * return [] if selector has no match
    * @param selector css selector of all elements,
    * @return enclosed text as a sequence of strings
    */
  def text(
            selector: String,
            own: Boolean = false
            ): Seq[String] = doc match {
    case Some(doc: Element) =>
      val elements = doc.select(selector)

      val result = if (!own) elements.map (_.text)
      else elements.map(_.ownText)

      result

    case _ => Seq[String]()
  }

  def boilerPipe(): String = doc match {
    case Some(doc: Document) =>

      ArticleExtractor.INSTANCE.getText(doc.html());

    case _ => null
  }

  //TODO: abomination
  //only slice contents inside the container, other parts are discarded
  //this will generate doc from scratch but otherwise induces heavy load on serialization
  //sliced page should not be saved. This function will be removed soon.
  def slice(
             selector: String,
             expand :Int = 0
             )(
             limit: Int
             ): Seq[Page] = {

    doc match {

      case Some(doc: Element) =>
        val elements = doc.select(selector)
        val length = Math.min(elements.size, limit)

        elements.subList(0, length).zipWithIndex.map {
          tuple => {

            this.copy(
              url = this.url + "#" + tuple._2,
              content = ("<table>"+tuple._1.outerHtml()+"</table>").getBytes(parsedContentType.getCharset)//otherwise tr and td won't be parsed
            )
          }
        }

      case _ => Seq[Page]()
    }
  }
}