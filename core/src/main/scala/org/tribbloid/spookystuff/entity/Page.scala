package org.tribbloid.spookystuff.entity

import java.io._
import java.util.{Date, UUID}

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.http.entity.ContentType
import org.jsoup.Jsoup
import org.jsoup.nodes.Element
import org.tribbloid.spookystuff.Const

import scala.collection.JavaConversions._
import scala.collection.immutable.ListMap

/**
 * Created by peng on 04/06/14.
 */

//immutable! we don't want to lose old pages
//keep small, will be passed around by Spark
case class Page(
                 resolvedUrl: String,
                 content: Array[Byte],
                 contentType: String,

                 backtrace: Array[Interactive] = null, //immutable, also the uid
                 timestamp: Date = new Date,
                 filePath: String = null
                 )
  extends Serializable {

  @transient lazy val parsedContentType: ContentType = {
    var result = ContentType.parse(this.contentType)
    if (result.getCharset == null) result = result.withCharset(Const.defaultCharset)
    result
  }
  @transient lazy val contentStr: String = new String(this.content,this.parsedContentType.getCharset)

  @transient lazy val doc: Option[Any] = if (parsedContentType.getMimeType.contains("html")){
    Option(Jsoup.parse(this.contentStr, resolvedUrl)) //not serialize, parsing is faster
  }
  else{
    None
  }

  def isExpired = new Date().getTime - timestamp.getTime > Const.pageExpireAfter * 1000

  //this will lose information as charset encoding will be different
  def save(
            path: String,
            overwrite: Boolean = false
            )(hConf: Configuration
    ): Page = {

    var fullPath = new Path(path)

    val fs = fullPath.getFileSystem(hConf)

    if (!overwrite && fs.exists(fullPath)) {
      fullPath = new Path(path +"-"+ UUID.randomUUID())
    }
    val fos = fs.create(fullPath, overwrite)

    IOUtils.write(content,fos)
    fos.close()


    this.copy(filePath = fullPath.getName)
  }

  def saveLocal(
                 path: String,
                 overwrite: Boolean = false
                 ): Page = {

//    val path: File = new File(dir)
//    if (!path.isDirectory) path.mkdirs()
//
//    val fullPathString = getFilePath(fileName, dir)

    var file: File = new File(path)

    if (!overwrite && file.exists()) {
      file = new File(path +"-"+ UUID.randomUUID())
    }

    file.createNewFile()

    val fos = new FileOutputStream(file)

    IOUtils.write(content,fos)
    fos.close()

    this.copy(filePath = "file://" + file.getAbsolutePath)
  }

  //  def refresh(): Page = {
  //    val page = PageBuilder.resolveFinal(this.backtrace: _*).modify(this.alias,this.context)
  //    return page
  //  }

  def elementExist(selector: String): Boolean = doc match {

    case Some(doc: Element) => !doc.select(selector).isEmpty

    case _ => false
  }

  def attrExist(
                 selector: String,
                 attr: String
                 ): Boolean = doc match {

    case Some(doc: Element) => elementExist(selector) && doc.select(selector).hasAttr(attr)

    case _ => false
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
             noEmpty: Boolean = true
             ): String = this.attr(selector, attr, noEmpty).headOption.orNull

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
             own: Boolean = false
             ): String = this.text(selector, own).headOption.orNull

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

  def extractAsMap[T](keyAndF: (String, Page => T)*): ListMap[String, T] = {
    ListMap(
      keyAndF.map{
        tuple => (tuple._1, tuple._2(this))
      }: _*
    )
  }

  def crawl1(
              action: ClientAction,
              f: Page => _
              ): PageRow = {

    f(this) match {
      case null => DeadRow
      case s: Any =>
        val fa = action.interpolate(Map("~" -> s))
        PageRow(actions = Seq(fa))
    }
  }

  def crawl(
             action: ClientAction,
             f: Page => Array[_]
             )(
             distinct: Boolean = true,
             limit: Int = Const.fetchLimit,
             indexKey: String = null
             ): Array[PageRow] = {

    val attrs = f(this)

    if (attrs.isEmpty) return Array(DeadRow)

    var actions = attrs.map( attr => action.interpolate(Map("~" -> attr)))

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
          PageRow(cells = ListMap(indexKey -> tuple._2),actions = Seq(tuple._1))
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
             limit: Int = Const.fetchLimit
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