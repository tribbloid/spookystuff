package org.tribbloid.spookystuff.entity

import java.util.Date
import org.tribbloid.spookystuff.conf.Conf

import org.jsoup.Jsoup
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import java.text.DateFormat
import java.io.{FileWriter, BufferedWriter, File, Serializable}
import java.util
import org.jsoup.nodes.Element
;

/**
 * Created by peng on 04/06/14.
 */

//immutable! we don't want to lose old pages
//keep small, will be passed around by Spark

//TODO: right now everything delegated to HtmlPage, more will come (e.g. PdfPage, SliceView, ImagePage, JsonPage)
abstract class Page extends Serializable {
  def exist(selector: String): Boolean

  def attrFirst(selector: String, attr: String): String

  def attrAll(selector: String, attr: String): Seq[String]

  def linkFirst(selector: String, absolute: Boolean = true): String

  def linkAll(selector: String, absolute: Boolean = true): Seq[String]

  def textFirst(selector: String): String

  def textAll(selector: String): Seq[String]
}

//I'm always using the more familiar Java collection, also for backward compatibility
class HtmlPage(
            val resolvedUrl: String,
            val content: String,

            val alias: String = null,

            val backtrace: util.List[Interaction] = new util.ArrayList[Interaction], //also the uid
            val context: util.Map[String, Serializable] = null, //I know it should be a var, but better save than sorry
            val timestamp: Date = new Date
            )
  extends Page {

  //share context. TODO: too many shallow copy making it dangerous
  //  def this(another: Page) = this (
  //      another.content,
  //      another.datetime,
  //      another.context)

  @transient lazy val doc: Element = Jsoup.parse(content, resolvedUrl) //not serialize, parsing is faster

  override def clone(): HtmlPage = new HtmlPage(
    this.resolvedUrl,
    this.content,
    this.alias,
    this.backtrace,
    this.context,
    this.timestamp
  )

  def modify(alias: String = this.alias, context: util.Map[String, Serializable] = this.context): HtmlPage = new HtmlPage(
    this.resolvedUrl,
    this.content,
    alias,
    this.backtrace,
    context,
    this.timestamp
  )

  //only slice contents inside the container, other parts are discarded
  //this will generate doc from scratch but otherwise induces heavy load on serialization
  def slice(selector: String): Seq[HtmlPage] = {
    val elements = doc.select(selector)
    return elements.zipWithIndex.map {
      elementWithIndex =>{
        new HtmlPage (
          this.resolvedUrl + "#" + elementWithIndex._2,
          elementWithIndex._1.html(),
          null,
          this.backtrace,
          this.context.clone().asInstanceOf,
          this.timestamp
        )
      }
    }
  }

  def isExpired = (new Date().getTime - timestamp.getTime > Conf.pageExpireAfter*1000)

  def refresh(): HtmlPage = {
    val page = PageBuilder.resolveFinal(this.backtrace: _*).modify(this.alias,this.context)
    return page
  }

  override def exist(selector: String): Boolean = {
    !doc.select(selector).isEmpty
  }

  override def attrFirst(selector: String, attr: String): String = {
    val element = doc.select(selector).first()
    if (element == null) null
    else element.attr(attr)
  }

  override def attrAll(selector: String, attr: String): Seq[String] = {
    val result = ArrayBuffer[String]()

    doc.select(selector).foreach{
      element => result += element.attr(attr)
    }

    return result.toSeq
  }

  override def linkFirst(selector: String, absolute: Boolean = true): String = {
    if (absolute == true) attrFirst(selector,"abs:href")
    else attrFirst(selector,"href")
  }

  override def linkAll(selector: String, absolute: Boolean = true): Seq[String] = {
    if (absolute == true) attrAll(selector,"abs:href")
    else attrAll(selector,"href")
  }

  override def textFirst(selector: String): String = {
    val element = doc.select(selector).first()
    if (element == null) null
    else element.text
  }

  override def textAll(selector: String): Seq[String] = {
    val result = ArrayBuffer[String]()

    doc.select(selector).foreach{
      element => result += element.text
    }

    return result.toSeq
  }

  def asMap(keyAndF: (String, HtmlPage => Serializable)*): util.Map[String, Serializable] = {
    val result: util.Map[String, Serializable] = new util.HashMap()

    keyAndF.foreach {
      fEntity => {
        val value = fEntity._2(this)
        result.put(fEntity._1, value)
      }
    }
    result
  }

  def save(namePattern: String = this.hashCode().toString, path: String = Conf.savePagePath, usePattern: Boolean = false) {
    var name = namePattern
    if (usePattern == true) {
      name = name.replace("#{time}", DateFormat.getInstance.format(this.timestamp))
      name = name.replace("#{resolved-url}", this.resolvedUrl)
    }

    //sanitizing filename can save me a lot of trouble
    name = name.replaceAll("[:\\\\/*?|<>]+", "_")

    val dir: File = new File(path)
    if (!dir.isDirectory) dir.mkdirs()

    val file: File = new File(path, name)
    if (!file.exists) file.createNewFile();

    val fw = new FileWriter(file.getAbsoluteFile());
    val bw = new BufferedWriter(fw);
    bw.write(content);
    bw.close();
  }
}