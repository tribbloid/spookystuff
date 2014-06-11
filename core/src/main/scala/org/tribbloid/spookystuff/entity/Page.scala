package org.tribbloid.spookystuff.entity

import java.util.Date
import org.tribbloid.spookystuff.conf.Conf

import org.jsoup.Jsoup
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import java.text.DateFormat
import java.io.{FileWriter, BufferedWriter, File}
;

/**
 * Created by peng on 04/06/14.
 */

//immutable! we don't want to lose old pages
//keep small, will be passed around by Spark


class Page(
            val resolvedUrl: String,
            val content: String,
            val datetime: Date = new Date
            //          like this: {submitTime: 10s, visitTime: 20s}
            )
  extends Serializable {// don't modify content!

  //share context. TODO: too many shallow copy making it dangerous
  //  def this(another: Page) = this (
  //      another.content,
  //      another.datetime,
  //      another.context)

  @transient val doc = Jsoup.parse(content) //not serialize, parsing is faster

  def isExpired = (new Date().getTime - datetime.getTime > Conf.pageExpireAfter*1000)

  def firstAttr(selector: String, attr: String): String = {
    val element = doc.select(selector).first()
    if (element == null) null
    else element.attr(attr)
  }

  def allAttrs(selector: String, attr: String): Seq[String] = {
    val result = ArrayBuffer[String]()

    doc.select(selector).foreach{
      element => result += element.attr(attr)
    }

    return result.toSeq
  }

  def firstLink(selector: String): String = firstAttr(selector,"href")

  def allLinks(selector: String): Seq[String] = allAttrs(selector,"href")

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

  def save(namePattern: String, path: String = Conf.savePagePath, usePattern: Boolean = false) {
    var name = namePattern
    if (usePattern == true) {
      name = name.replaceAll("#{time}", DateFormat.getInstance.format(this.datetime))
      name = name.replaceAll("#{resolved-url}", this.resolvedUrl)
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



