package org.tribbloid.spookystuff.entity

import java.io._
import java.text.DateFormat
import java.util
import java.util.{Date, UUID}

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.http.entity.ContentType
import org.jsoup.Jsoup
import org.jsoup.nodes.Element
import org.tribbloid.spookystuff.Const

import scala.collection.JavaConversions._

/**
 * Created by peng on 04/06/14.
 */

//immutable! we don't want to lose old pages
//keep small, will be passed around by Spark
//I'm always using the more familiar Java collection, also for backward compatibility
//TODO: Java convention or Scala conventions?
case class Page(
                 val resolvedUrl: String,
                 val content: Array[Byte],
                 val contentType: String,

                 val alias: String = null,

                 val backtrace: Array[Interactive] = new Array(0), //immutable, also the uid
                 val context: util.LinkedHashMap[String, Serializable] = new util.LinkedHashMap, //Mutable! caused a lot of headache
                 val timestamp: Date = new Date,
                 val savePath: String = null
                 )
  extends Serializable{

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

  def isExpired = (new Date().getTime - timestamp.getTime > Const.pageExpireAfter*1000)

  def getFilePath(fileName: String = "#{resolved-url}", dir: String = Const.savePagePath): String ={
    var formattedFileName = ClientAction.formatWithContext(fileName, this.context)

    formattedFileName = formattedFileName.replace("#{resolved-url}", this.resolvedUrl)
    formattedFileName = formattedFileName.replace("#{timestamp}", DateFormat.getInstance.format(this.timestamp))

    //sanitizing filename can save me a lot of trouble
    formattedFileName = formattedFileName.replaceAll("[:\\\\/*?|<>_]+", ".")
    if (formattedFileName.length>200) formattedFileName = formattedFileName.substring(0,200) //max fileName length is 256

    var formattedDir = dir
    if (!formattedDir.endsWith("/")) formattedDir = dir+"/"
    val dirPath = new Path(formattedDir)
    return new Path(dirPath, formattedFileName).toString
  }

  //this will lose information as charset encoding will be different
  def save(fileName: String = "#{resolved-url}", dir: String = Const.savePagePath, overwrite: Boolean = false)(hConf: Configuration): String = {

    //    val path = new Path(dir)

    //TODO: slow to check if the dir exist
    //    val fs = path.getFileSystem(hConf)
    //    if (!fs.isDirectory(path)) {
    //      if (!fs.mkdirs(path)) {
    //        throw new SparkException("Failed to create save path " + path) //TODO: Still SparkException?
    //      }
    //    }

    val fullPathString = getFilePath(fileName, dir)
    var fullPath = new Path(fullPathString)

    val fs = fullPath.getFileSystem(hConf)

    if (overwrite==false && fs.exists(fullPath)) {
      fullPath = new Path(fullPathString +"_"+ UUID.randomUUID())
    }
    val fos = fs.create(fullPath, overwrite) //don't overwrite important file

    //    val writer = new BufferedWriter(new OutputStreamWriter(fileOutputStream,"UTF-8")) //why using two buffers

    IOUtils.write(content,fos)
    fos.close()

    return fullPath.getName
  }

  def saveLocal(fileName: String = "#{resolved-url}", dir: String = Const.localSavePagePath, overwrite: Boolean = false): String = {

    val path: File = new File(dir)
    if (!path.isDirectory) path.mkdirs()

    val fullPathString = getFilePath(fileName, dir)

    var file: File = new File(fullPathString)

    if (overwrite==false && file.exists()) {
      file = new File(fullPathString +"_"+ UUID.randomUUID())
    }

    file.createNewFile();

    val fos = new FileOutputStream(file)

    IOUtils.write(content,fos)
    fos.close()

    return file.getAbsolutePath
  }

  def asJson(): String = {
    jsonMapper.writeValueAsString(this.context)
  }

  //only slice contents inside the container, other parts are discarded
  //this will generate doc from scratch but otherwise induces heavy load on serialization
  def slice(
             selector: String,
             alias: String = null,
             limit: Int = Const.fetchLimit,
             indexKey: String = null
             ): Seq[Page] = doc match {

    case Some(doc: Element) => {
      val elements = doc.select(selector)
      val length = Math.min(elements.size, limit)

      var newAlias = this.alias
      if (alias != null) newAlias = alias

      return elements.subList(0, length).zipWithIndex.map {
        tuple => {
          val context = new util.LinkedHashMap[String,Serializable](this.context)

          if (indexKey!=null) {
            context.put(indexKey, tuple._2)
          }

          this.copy(
            resolvedUrl = this.resolvedUrl + "#" + tuple._2,
            content = ("<table>"+tuple._1.html()+"</table>").getBytes(parsedContentType.getCharset),//otherwise tr and td won't be parsed
            alias = newAlias,
            context = context
          )
        }
      }
    }

    case _ => return Seq[Page]()

  }

  //  def refresh(): Page = {
  //    val page = PageBuilder.resolveFinal(this.backtrace: _*).modify(this.alias,this.context)
  //    return page
  //  }

  def elementExist(selector: String): Boolean = doc match {

    case Some(doc: Element) => !doc.select(selector).isEmpty

    case _ => return false
  }

  def attrExist(selector: String, attr: String): Boolean = doc match {

    case Some(doc: Element) => elementExist(selector) && doc.select(selector).hasAttr(attr)

    case _ => return false
  }

  /**
   * Return attribute of an element.
   * return null if selector has no match, return "" if it has a match but attribute doesn't exist
   * @param selector css selector of the element, only the first match will be return
   * @param attr attribute
   * @return value of the attribute as string
   */
  def attr1(selector: String, attr: String): String = doc match {
    case Some(doc: Element) => {

      val element = doc.select(selector).first()
      if (element == null) null
      else element.attr(attr)
    }

    case _ => null
  }

  /**
   * Return a sequence of attributes of all elements that match the selector.
   * return [] if selector has no match,
   * returned Sequence may contains "" for elements that match the selector but without required attribute, use filter if you don't want them
   * @param selector css selector of all elements
   * @param attr attribute
   * @param limit only the first n elements will be used
   * @param distinct whether to remove duplicate values
   * @return values of the attributes as a sequence of strings
   */
  def attr(selector: String, attr: String, limit: Int = Const.fetchLimit, distinct: Boolean = false): Seq[String] = doc match {
    case Some(doc: Element) => {

      val elements = doc.select(selector)
      val length = Math.min(elements.size, limit)

      val result = elements.subList(0, length).map {
        _.attr(attr)
      }

      if (distinct == true) return result.distinct
      else return result
    }

    case _ => Seq[String]()
  }

  /**
   * Shorthand for attr1("href")
   * @param selector css selector of the element
   * @param absolute whether to use absolute path (site url + relative path) or relative path, default to true
   * @return value of the attribute as string
   */
  def href1(selector: String, absolute: Boolean = true): String = {
    if (absolute == true) attr1(selector,"abs:href")
    else attr1(selector,"href")
  }

  /**
   * Shorthand for attr("href")
   * @param selector css selector of all elements
   * @param limit only the first n elements will be used
   * @param absolute whether to use absolute path (site url + relative path) or relative path, default to true
   * @param distinct whether to remove duplicate values
   * @return values of the attributes as a sequence of strings
   */
  def href(selector: String, limit: Int = Const.fetchLimit, absolute: Boolean = true, distinct: Boolean = false): Seq[String] = {
    if (absolute == true) attr(selector,"abs:href",limit,distinct)
    else attr(selector,"href",limit,distinct)
  }

  /**
   * Shorthand for attr1("src")
   * @param selector css selector of the element
   * @param absolute whether to use absolute path (site url + relative path) or relative path, default to true
   * @return value of the attribute as string
   */
  def src1(selector: String, absolute: Boolean = true): String = {
    if (absolute == true) attr1(selector,"abs:src")
    else attr1(selector,"src")
  }

  /**
   * Shorthand for attr("src")
   * @param selector css selector of all elements
   * @param limit only the first n elements will be used
   * @param absolute whether to use absolute path (site url + relative path) or relative path, default to true
   * @param distinct whether to remove duplicate values
   * @return values of the attributes as a sequence of strings
   */
  def src(selector: String, limit: Int = Const.fetchLimit, absolute: Boolean = true, distinct: Boolean = false): Seq[String] = {
    if (absolute == true) attr(selector,"abs:src",limit,distinct)
    else attr(selector,"src",limit,distinct)
  }

  //return null if selector found nothing, return "" if found something without text
  /**
   * Return all text enclosed by an element.
   * return null if selector has no match
   * @param selector css selector of the element, only the first match will be return
   * @return enclosed text as string
   */
  def text1(selector: String): String = doc match {
    case Some(doc: Element) => {
      val element = doc.select(selector).first()
      if (element == null) null
      else element.text
    }

    case _ => null
  }

  /** Return an array of texts enclosed by their respective elements
    * return [] if selector has no match
    * @param selector css selector of all elements,
    * @param limit only the first n elements will be used
    * @param distinct whether to remove duplicate values
    * @return enclosed text as a sequence of strings
    */
  def text(selector: String, limit: Int = Const.fetchLimit, distinct: Boolean = false): Seq[String] = doc match {
    case Some(doc: Element) => {
      val elements = doc.select(selector)
      val length = Math.min(elements.size, limit)

      val result = elements.subList(0, length).map {
        _.text
      }

      if (distinct == true) return result.distinct
      else return result
    }

    case _ => Seq[String]()
  }

  def ownText1(selector: String): String = doc match {
    case Some(doc: Element) => {
      val element = doc.select(selector).first()
      if (element == null) null
      else element.ownText()
    }

    case _ => null
  }

  def ownText(selector: String, limit: Int = Const.fetchLimit, distinct: Boolean = false): Seq[String] = doc match {
    case Some(doc: Element) => {
      val elements = doc.select(selector)
      val length = Math.min(elements.size, limit)

      val result = elements.subList(0, length).map {
        _.ownText()
      }

      if (distinct == true) return result.distinct
      else return result
    }

    case _ => Seq[String]()
  }

  def extractPropertiesAsMap(keyAndF: (String, Page => Serializable)*): util.LinkedHashMap[String, Serializable] = {
    val result: util.LinkedHashMap[String, Serializable] = new util.LinkedHashMap()

    keyAndF.foreach {
      fEntity => {
        val value = fEntity._2(this)
        result.put(fEntity._1, value)
      }
    }
    result
  }
}

//object EmptyPage extends Page(
//  "about:empty",
//  new Array[Byte](0),
//  "text/html; charset=UTF-8"
//)