package org.tribbloid.spookystuff.entity

import java.nio.charset.Charset
import java.text.DateFormat
import java.util.Date

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, Path}
import org.apache.http.entity.ContentType
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.SparkException
import org.jsoup.Jsoup
import org.tribbloid.spookystuff.Conf
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import java.io.{OutputStreamWriter, Serializable}
import java.util
import org.jsoup.nodes.Element
;

/**
 * Created by peng on 04/06/14.
 */

//immutable! we don't want to lose old pages
//keep small, will be passed around by Spark
//I'm always using the more familiar Java collection, also for backward compatibility
class Page(
            val resolvedUrl: String,
            val content: Array[Byte],
            val contentType: String,

            val alias: String = null,

            val backtrace: util.List[Interaction] = new util.ArrayList[Interaction], //also the uid
            val context: util.Map[String, Serializable] = null, //I know it should be a var, but better save than sorry
            val timestamp: Date = new Date,

            val filePath: String = null
            )
  extends Serializable{

  //share context. TODO: too many shallow copy making it dangerous
  //  def this(another: Page) = this (
  //      another.content,
  //      another.datetime,
  //      another.context)

  @transient lazy val parsedContentType: ContentType = ContentType.parse(this.contentType)
  @transient lazy val contentStr: String = new String(this.content,this.parsedContentType.getCharset)
  @transient lazy val doc: Element = if (parsedContentType.getMimeType.contains("html")){
    Jsoup.parse(this.contentStr, resolvedUrl) //not serialize, parsing is faster
  }
  else{
    null
  }

  def isExpired = (new Date().getTime - timestamp.getTime > Conf.pageExpireAfter*1000)

  def copy(): Page = new Page(
    this.resolvedUrl,
    this.content,
    this.contentType,
    this.alias,
    this.backtrace,
    this.context,
    this.timestamp
  )

  def modify(alias: String = this.alias, context: util.Map[String, Serializable] = this.context): Page = new Page(
    this.resolvedUrl,
    this.content,
    this.contentType,
    alias,
    this.backtrace,
    context,
    this.timestamp
  )

  //only slice contents inside the container, other parts are discarded
  //this will generate doc from scratch but otherwise induces heavy load on serialization
  def slice(selector: String): Seq[Page] = {
    val elements = doc.select(selector)
    return elements.zipWithIndex.map {
      elementWithIndex =>{
        new Page (
          this.resolvedUrl + "#" + elementWithIndex._2,
          elementWithIndex._1.html().getBytes("UTF8"),
          this.contentType,
          null,
          this.backtrace,
          this.context,
          this.timestamp
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

  def attrFirst(selector: String, attr: String): String = {
    val element = doc.select(selector).first()
    if (element == null) null
    else element.attr(attr)
  }

  def attrAll(selector: String, attr: String): Seq[String] = {
    val result = ArrayBuffer[String]()

    doc.select(selector).foreach{
      element => result += element.attr(attr)
    }

    return result.toSeq
  }

  def linkFirst(selector: String, absolute: Boolean = true): String = {
    if (absolute == true) attrFirst(selector,"abs:href")
    else attrFirst(selector,"href")
  }

  def linkAll(selector: String, absolute: Boolean = true): Seq[String] = {
    if (absolute == true) attrAll(selector,"abs:href")
    else attrAll(selector,"href")
  }

  def textFirst(selector: String): String = {
    val element = doc.select(selector).first()
    if (element == null) null
    else element.text
  }

  def textAll(selector: String): Seq[String] = {
    val result = ArrayBuffer[String]()

    doc.select(selector).foreach{
      element => result += element.text
    }

    return result.toSeq
  }

  def asMap(keyAndF: (String, Page => Serializable)*): util.Map[String, Serializable] = {
    val result: util.Map[String, Serializable] = new util.HashMap()

    keyAndF.foreach {
      fEntity => {
        val value = fEntity._2(this)
        result.put(fEntity._1, value)
      }
    }
    result
  }

  //this is only for sporadic file saving, will cause congestion if used in a full-scale transformation.
  //If you want to save everything in an RDD, use actions like RDD.save...()
  //also remember this will lose information as charset encoding will be different
  def save(fileName: String = "#{resolved-url}_"+this.hashCode(), dir: String = Conf.savePagePath, overwrite: Boolean = false): String = {
    var formattedFileName = Action.formatWithContext(fileName, this.context)

    formattedFileName = formattedFileName.replace("#{resolved-url}", this.resolvedUrl)
    formattedFileName = formattedFileName.replace("#{timestamp}", DateFormat.getInstance.format(this.timestamp))

    //sanitizing filename can save me a lot of trouble
    formattedFileName = formattedFileName.replaceAll("[:\\\\/*?|<>]+", "_")

    val path = new Path(dir)

    //TODO: slow to check if the dir exist
    val fs = path.getFileSystem(Conf.hConf.value)
    if (!fs.isDirectory(path)) {
      if (!fs.mkdirs(path)) {
        throw new SparkException("Failed to create save path " + path) //TODO: Still SparkException?
      }
    }

    var fullPath = new Path(path, formattedFileName)

    if (overwrite==false && fs.exists(fullPath)) {
      fullPath = new Path(path, formattedFileName + this.hashCode())
    }
    val fos = fs.create(fullPath, overwrite) //don't overwrite important file

    //    val writer = new BufferedWriter(new OutputStreamWriter(fileOutputStream,"UTF-8")) //why using two buffers

    IOUtils.write(content,fos)
    fos.close()

    return fullPath.getName
  }
}