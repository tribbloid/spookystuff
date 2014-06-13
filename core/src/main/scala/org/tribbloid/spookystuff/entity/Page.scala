package org.tribbloid.spookystuff.entity

import java.util.Date
import org.tribbloid.spookystuff.conf.Conf

import org.jsoup.Jsoup
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import java.text.DateFormat
import java.io.{FileWriter, BufferedWriter, File}
import java.util
;

/**
 * Created by peng on 04/06/14.
 */

//immutable! we don't want to lose old pages
//keep small, will be passed around by Spark


//I'm always using the more familiar Java collection, also for backward compatibility
class Page(
            val resolvedUrl: String,
            val content: String,

            val alias: String = null,

            val backtrace: util.List[Interaction] = new util.ArrayList[Interaction], //also the uid
            var context: util.Map[String,String] = null, //will NOT be carried over to the linked page to avoid diamond
            val datetime: Date = new Date
            )
  extends Serializable {

  //share context. TODO: too many shallow copy making it dangerous
  //  def this(another: Page) = this (
  //      another.content,
  //      another.datetime,
  //      another.context)

  @transient lazy val doc = Jsoup.parse(content) //not serialize, parsing is faster

  override def clone(): Page = new Page(
    this.resolvedUrl,
    this.content,
    this.alias,
    this.backtrace,
    this.context,
    this.datetime
  )

  def as(as: String): Page = new Page(
    this.resolvedUrl,
    this.content,
    as,
    this.backtrace,
    this.context,
    this.datetime
  )

  def isExpired = (new Date().getTime - datetime.getTime > Conf.pageExpireAfter*1000)

  def refresh(): Page =  PageBuilder.resolveFinal(this.backtrace: _*)(this.context).as(this.alias)

  def exist(selector: String): Boolean = {
    !doc.select(selector).isEmpty
  }

  def firstAttr(selector: String, attr: String): String = {
    val element = doc.select(selector).first()
    if (element == null) null
    else element.attr(attr)
  }

  def attr(selector: String, attr: String): Seq[String] = {
    val result = ArrayBuffer[String]()

    doc.select(selector).foreach{
      element => result += element.attr(attr)
    }

    return result.toSeq
  }

  def firstLink(selector: String): String = firstAttr(selector,"href")

  def allLinks(selector: String): Seq[String] = attr(selector,"href")

  def firstText(selector: String): String = {
    val element = doc.select(selector).first()
    if (element == null) null
    else element.text
  }

  def allTexts(selector: String): Seq[String] = {
    val result = ArrayBuffer[String]()

    doc.select(selector).foreach{
      element => result += element.text
    }

    return result.toSeq
  }

  def save(namePattern: String = this.hashCode().toString, path: String = Conf.savePagePath, usePattern: Boolean = false) {
    var name = namePattern
    if (usePattern == true) {
      name = name.replace("#{time}", DateFormat.getInstance.format(this.datetime))
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
  //  def slice(selector: String): Seq[Page] = {
  //    val slices = doc.select(selector).
  //  }
}



