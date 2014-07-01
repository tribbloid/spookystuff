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
import org.tribbloid.spookystuff.Conf

import scala.collection.JavaConversions._
;

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

            val backtrace: Array[Interactive] = null, //immutable, also the uid
            val context: util.HashMap[String, Serializable] = null, //Mutable! caused a lot of headache
            val timestamp: Date = new Date, //immutable
            val savePath: String = null
            )
  extends Serializable{

  //share context. TODO: too many shallow copy making it dangerous
  //  def this(another: Page) = this (
  //      another.content,
  //      another.datetime,
  //      another.context)

  @transient lazy val parsedContentType: ContentType = ContentType.parse(this.contentType)
  @transient lazy val contentStr: String = {
    if (this.parsedContentType.getCharset == null) {
      new String(this.content, Conf.defaultCharset)
    }
    else
    {
      new String(this.content,this.parsedContentType.getCharset)
    }
  }
  @transient lazy val doc: Element = if (parsedContentType.getMimeType.contains("html")){
    Jsoup.parse(this.contentStr, resolvedUrl) //not serialize, parsing is faster
  }
  else{
    null
  }

  def isExpired = (new Date().getTime - timestamp.getTime > Conf.pageExpireAfter*1000)

  //only slice contents inside the container, other parts are discarded
  //this will generate doc from scratch but otherwise induces heavy load on serialization
  def slice(selector: String, alias: String = null): Seq[Page] = {
    val elements = doc.select(selector)

    var newAlias = this.alias
    if (alias != null) newAlias = alias

    return elements.zipWithIndex.map {
      elementWithIndex =>{
        this.copy(
          resolvedUrl = this.resolvedUrl + "#" + elementWithIndex._2,
          content = elementWithIndex._1.html().getBytes(parsedContentType.getCharset),
          alias = newAlias,
          context = new util.HashMap(this.context)
        )
      }
    }
  }

  //  def refresh(): Page = {
  //    val page = PageBuilder.resolveFinal(this.backtrace: _*).modify(this.alias,this.context)
  //    return page
  //  }

  def elementExist(selector: String): Boolean = {
    !doc.select(selector).isEmpty
  }

  def attrExist(selector: String, attr: String): Boolean = {
    elementExist(selector) && doc.select(selector).hasAttr(attr)
  }

  def attr1(selector: String, attr: String): String = {
    val element = doc.select(selector).first()
    if (element == null) null
    else element.attr(attr)
  }

  def attr(selector: String, attr: String, limit: Int = Conf.fetchLimit, distinct: Boolean = false): Seq[String] = {
    val elements = doc.select(selector)
    val length = Math.min(elements.size,limit)

    val result = elements.subList(0,length).map {
      _.attr(attr)
    }

    if (distinct == true) return result.distinct
    else return result
  }

  def href1(selector: String, absolute: Boolean = true): String = {
    if (absolute == true) attr1(selector,"abs:href")
    else attr1(selector,"href")
  }

  def href(selector: String, limit: Int = Conf.fetchLimit, absolute: Boolean = true, distinct: Boolean = false): Seq[String] = {
    if (absolute == true) attr(selector,"abs:href",limit,distinct)
    else attr(selector,"href",limit,distinct)
  }

  def src1(selector: String, absolute: Boolean = true): String = {
    if (absolute == true) attr1(selector,"abs:src")
    else attr1(selector,"src")
  }

  def src(selector: String, limit: Int = Conf.fetchLimit, absolute: Boolean = true, distinct: Boolean = false): Seq[String] = {
    if (absolute == true) attr(selector,"abs:src",limit,distinct)
    else attr(selector,"src",limit,distinct)
  }

  def text1(selector: String): String = {
    val element = doc.select(selector).first()
    if (element == null) null
    else element.text
  }

  def text(selector: String, limit: Int = Conf.fetchLimit, distinct: Boolean = false): Seq[String] = {
    val elements = doc.select(selector)
    val length = Math.min(elements.size,limit)

    val result = elements.subList(0,length).map {
      _.text
    }

    if (distinct == true) return result.distinct
    else return result
  }

  def extractPropertiesAsMap(keyAndF: (String, Page => Serializable)*): util.HashMap[String, Serializable] = {
    val result: util.HashMap[String, Serializable] = new util.HashMap()

    keyAndF.foreach {
      fEntity => {
        val value = fEntity._2(this)
        result.put(fEntity._1, value)
      }
    }
    result
  }

  def getFilePath(fileName: String = "#{resolved-url}", dir: String = Conf.savePagePath): String ={
    var formattedFileName = ActionUtils.formatWithContext(fileName, this.context)

    formattedFileName = formattedFileName.replace("#{resolved-url}", this.resolvedUrl)
    formattedFileName = formattedFileName.replace("#{timestamp}", DateFormat.getInstance.format(this.timestamp))

    //sanitizing filename can save me a lot of trouble
    formattedFileName = formattedFileName.replaceAll("[:\\\\/*?|<>_]+", ".")

    var formattedDir = dir
    if (!formattedDir.endsWith("/")) formattedDir = dir+"/"
    val dirPath = new Path(formattedDir)
    return new Path(dirPath, formattedFileName).toString
  }

  //this is only for sporadic file saving, will cause congestion if used in a full-scale transformation.
  //If you want to save everything in an RDD, use actions like RDD.save...()
  //also remember this will lose information as charset encoding will be different
  def save(fileName: String = "#{resolved-url}", dir: String = Conf.savePagePath, overwrite: Boolean = false)(hConf: Configuration): String = {

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

  def saveLocal(fileName: String = "#{resolved-url}", dir: String = Conf.localSavePagePath, overwrite: Boolean = false): String = {

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
}