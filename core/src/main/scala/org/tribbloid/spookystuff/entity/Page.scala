package org.tribbloid.spookystuff.entity

import java.io._
import java.util.{Date, UUID}

import de.l3s.boilerpipe.extractors.ArticleExtractor
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.http.entity.ContentType
import org.apache.spark.{SparkEnv, SparkContext, SparkConf}
import org.apache.spark.serializer.JavaSerializer
import org.jsoup.Jsoup
import org.jsoup.nodes.{Document, Element}
import org.tribbloid.spookystuff.entity.client.Action
import org.tribbloid.spookystuff.{Const, SpookyContext, Utils}

import scala.collection.JavaConversions._

object Page {

  def load(fullPath: Path)(hConf: Configuration): Array[Byte] = {

    val fs = fullPath.getFileSystem(hConf)

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

  //unlike save, this will store all information in an unreadable, serialized, probably compressed file
  def cache(
             pages: Seq[Page],
             path: String,
             overwrite: Boolean = false
             )(hConf: Configuration): Unit = {

    var fullPath = new Path(path)

    val fs = fullPath.getFileSystem(hConf)

    if (!overwrite && fs.exists(fullPath)) fullPath = new Path(path +"-"+ UUID.randomUUID())

    val ser = SparkEnv.get.serializer.newInstance()
    val copy = ser.serialize(pages)

    val fos = fs.create(fullPath, overwrite)
    try {
      fos.write(copy.array())
    }
    finally {
      fos.close()
    }
  }

  def autoCache(
                 pages: Seq[Page],
                 uid: PageUID,
                 spooky: SpookyContext
                 ): Unit = {
    val pathStr = Utils.urlConcat(
      spooky.autoCacheRoot,
      spooky.autoCacheLookup(uid).toString,
      UUID.randomUUID().toString
    )

    Page.cache(pages, pathStr)(spooky.hConf)
  }

  def restore(fullPath: Path)(hConf: Configuration): Seq[Page] = {

    val fs = fullPath.getFileSystem(hConf)

    if (fs.exists(fullPath)) {
      val fis = fs.open(fullPath)

      val ser = SparkEnv.get.serializer.newInstance()

      val serIn = ser.deserializeStream(fis)
      val obj = serIn.readObject[Seq[Page]]()
      serIn.close()
      obj
    }
    else null
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
                     )(hConf: Configuration): Seq[Page] = {

    val fs = dirPath.getFileSystem(hConf)

    if (fs.exists(dirPath) && fs.getFileStatus(dirPath).isDir) {
      //      val results = new ArrayBuffer[Page]()

      val statuses = fs.listStatus(dirPath)

      val latestStatus = statuses.filter(status => !status.isDir && status.getModificationTime >= earliestModificationTime).sortBy(_.getModificationTime).lastOption

      latestStatus match {
        case Some(status) => restore(status.getPath)(hConf)
        case _ => null
      }
    }
    else null
  }

  //TODO: cannot handle infinite duration, avoid using it!
  def autoRestoreLatest(
                         uid: PageUID,
                         spooky: SpookyContext
                         ): Seq[Page] = {
    val pathStr = Utils.urlConcat(
      spooky.autoCacheRoot,
      spooky.autoCacheLookup(uid).toString
    )

    restoreLatest(new Path(pathStr), System.currentTimeMillis() - spooky.pageExpireAfter.toMillis)(spooky.hConf)
  }
}

/**
 * Created by peng on 04/06/14.
 */
//use to genterate a lookup key for each page so
case class PageUID(
                    backtrace: Seq[Action],
                    blockKey: Int = -1 //-1 is no sub key
                    )

//immutable! we don't want to lose old pages
//keep small, will be passed around by Spark
@SerialVersionUID(1925602137496052L)
case class Page(
                 uid: PageUID,

                 resolvedUrl: String,
                 contentType: String,
                 content: Array[Byte],

                 name: String = null,
                 //                 cookie: Seq[SerializableCookie] = Seq(),
                 timestamp: Date = new Date,
                 saved: String = null
                 )
  extends Serializable {

//  private final val serialVersionUID: Long = 1925602137496052L

  @transient lazy val parsedContentType: ContentType = {
    var result = ContentType.parse(this.contentType)
    if (result.getCharset == null) result = result.withCharset(Const.defaultCharset)
    result
  }
  @transient lazy val contentStr: String = new String(this.content,this.parsedContentType.getCharset)

  @transient lazy val doc: Option[Any] = if (parsedContentType.getMimeType.contains("html")){
    Some(Jsoup.parse(this.contentStr, resolvedUrl)) //not serialize, parsing is faster
  }
  else{
    None
  }

  def backtrace = this.uid.backtrace
  def blockKey = this.uid.blockKey

  //this will lose information as charset encoding will be different
  def save(
            pathParts: Seq[String],
            overwrite: Boolean = false
            //            metadata: Boolean = true
            )(hConf: Configuration): Page = {

    val path = Utils.urlConcat(pathParts: _*)

    var fullPath = new Path(path)

    val fs = fullPath.getFileSystem(hConf)

    if (!overwrite && fs.exists(fullPath)) fullPath = new Path(path +"-"+ UUID.randomUUID())

    val fos = fs.create(fullPath, overwrite)

    try {
      IOUtils.write(content,fos)
    }
    finally {
      fos.close()
    }

    this.copy(saved = fullPath.toString)
  }



  //  private def autoPath[T](
  //                           root: String,
  //                           lookup: Lookup,
  //                           extract: Extract[_]
  //                           ): String = {
  //
  //    if (!root.endsWith("/")) root + "/" + lookup(backtrace,resolvedUrl) + "/" + extract(this)
  //    else root + lookup(backtrace,resolvedUrl) + "/" + extract(this)
  //  }

  def autoSave(
                spooky: SpookyContext,
                overwrite: Boolean = false
                ): Page = this.save(
    spooky.autoSaveRoot :: spooky.autoSaveExtract(this).toString :: Nil
  )(spooky.hConf)

  def errorDump(
                 spooky: SpookyContext,
                 overwrite: Boolean = false
                 ): Page = this.save(
    spooky.errorDumpRoot :: spooky.errorDumpExtract(this).toString :: Nil
  )(spooky.hConf)

  def localErrorDump(
                      spooky: SpookyContext,
                      overwrite: Boolean = false
                      ): Page = this.save(
    spooky.localErrorDumpRoot :: spooky.errorDumpExtract(this).toString :: Nil
  )(spooky.hConf)

  //  def saveLocal(
  //                 path: String,
  //                 overwrite: Boolean = false
  //                 ): Page = {
  //
  ////    val path: File = new File(dir)
  ////    if (!path.isDirectory) path.mkdirs()
  ////
  ////    val fullPathString = getFilePath(fileName, dir)
  //
  //    var file: File = new File(path)
  //
  //    if (!overwrite && file.exists()) {
  //      file = new File(path +"-"+ UUID.randomUUID())
  //    }
  //
  //    file.createNewFile()
  //
  //    val fos = new FileOutputStream(file)
  //
  //    IOUtils.write(content,fos)
  //    fos.close()
  //
  //    this.copy(savedTo = "file://" + file.getCanonicalPath)
  //  }

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
            ): Array[String] = doc match {
    case Some(doc: Element) =>

      val elements = doc.select(selector)

      val result = elements.map {
        _.attr(attr)
      }.toArray

      if (noEmpty) result.filter(_.nonEmpty)
      else result

    case _ => Array[String]()
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
            ): Array[String] = {
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
           ): Array[String] = {
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

  /** Return an array of texts enclosed by their respective elements
    * return [] if selector has no match
    * @param selector css selector of all elements,
    * @return enclosed text as a sequence of strings
    */
  def text(
            selector: String,
            own: Boolean = false
            ): Array[String] = doc match {
    case Some(doc: Element) =>
      val elements = doc.select(selector)

      val result = if (!own) elements.map (_.text)
      else elements.map(_.ownText)

      result.toArray

    case _ => Array[String]()
  }

  def boilerPipe(): String = doc match {
    case Some(doc: Document) =>

      ArticleExtractor.INSTANCE.getText(doc.html());

    case _ => null
  }

  def crawl1(
              action: Action,
              f: Page => _
              ): PageRow = {

    f(this) match {
      case null => DeadRow
      case s: Any =>
        val fa = action.interpolateFromMap(Map("~" -> s))
        PageRow(actions = Seq(fa))
    }
  }

  def crawl(
             action: Action,
             f: Page => Array[_]
             )(
             limit: Int,
             distinct: Boolean = true,
             indexKey: String = null
             ): Array[PageRow] = {

    val attrs = f(this)

    if (attrs.isEmpty) return Array(DeadRow)

    var actions = attrs.map( attr => action.interpolateFromMap(Map("~" -> attr)))

    if (distinct) actions = actions.distinct

    if (actions.size > limit) {
      actions = actions.slice(0,limit)
    }

    actions.zipWithIndex.map(
      tuple => {
        if (indexKey == null) {
          PageRow(actions = Seq(tuple._1))
        }
        else {
          PageRow(cells = Map(indexKey -> tuple._2),actions = Seq(tuple._1))
        }
      }
    ).toArray
  }

  //only slice contents inside the container, other parts are discarded
  //this will generate doc from scratch but otherwise induces heavy load on serialization
  //sliced page should not be saved. This function will be removed soon.
  def slice(
             selector: String,
             expand :Int = 0
             )(
             limit: Int
             ): Array[Page] = {

    doc match {

      case Some(doc: Element) =>
        val elements = doc.select(selector)
        val length = Math.min(elements.size, limit)

        elements.subList(0, length).zipWithIndex.map {
          tuple => {

            this.copy(
              resolvedUrl = this.resolvedUrl + "#" + tuple._2,
              content = ("<table>"+tuple._1.outerHtml()+"</table>").getBytes(parsedContentType.getCharset)//otherwise tr and td won't be parsed
            )
          }
        }.toArray

      case _ => Array[Page]()

    }
  }

}

//object EmptyPage extends Page(
//  "about:empty",
//  new Array[Byte](0),
//  "text/html; charset=UTF-8"
//)