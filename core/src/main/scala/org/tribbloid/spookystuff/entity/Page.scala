package org.tribbloid.spookystuff.entity

import java.util.Date
import org.tribbloid.spookystuff.conf.Conf
import org.openqa.selenium.phantomjs.PhantomJSDriver
import org.openqa.selenium.By
import org.openqa.selenium.support.ui
import org.jsoup.Jsoup
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import java.text.DateFormat
import java.io.{FileWriter, BufferedWriter, File}
import org.openqa.selenium.support.ui.{ExpectedConditions, WebDriverWait}
;

/**
 * Created by peng on 04/06/14.
 */

//immutable! we don't want to lose old pages
//keep small, will be passed around by Spark
case class Page(
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

  //  def first(selector: String): Element = {
  //    doc.select(selector).first()
  //  }
  //
  //  def all(selector: String): Elements = {
  //    doc.select(selector)
  //  }

  def firstLink(selector: String): String = {
    val element = doc.select(selector).first()
    if (element == null) null
    else element.attr("href")
  }

  def allLinks(selector: String): Seq[String] = {
    val result = ArrayBuffer[String]()

    doc.select(selector).foreach{
      element => result += element.attr("href")
    }

    return result.toSeq
  }

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

//TODO: need refactoring to accept more openqa drivers
private class PageBuilder {

  private val driver = new PhantomJSDriver(Conf.phantomJSCaps);
  //  private var urlBuilder: StringBuilder = null

  //by default driver should be reset in this case, but whatever
  //  def visit(url: String) {
  //    driver.get(url);
  //  }

  //  private def ifVisited(f: => Unit) = {if (urlBuilder!=null) f}

  //try to delegate all failover to Spark, but this may change in the future
  def interact(interaction: Interaction) = interaction match {
    case Visit(url) => {
      driver.get(url)
      //      this.urlBuilder = new StringBuilder(url)
    }
    case Delay(delay) => {
      Thread.sleep(delay*1000)
      //      this.urlBuilder.append(" ").append(interaction)
    }
    case DelayFor(selector, delay) => {
      val wait = new WebDriverWait(driver, delay)
      wait.until(ExpectedConditions.presenceOfAllElementsLocatedBy(By.cssSelector(selector)))
    }
    //TODO: still need nullPointerException handling!
    case Click(selector) => {
      driver.findElement(By.cssSelector(selector)).click()
      //      this.urlBuilder.append(" ").append(interaction)
    }
    case Submit(selector) => {
      driver.findElement(By.cssSelector(selector)).submit()
      //      this.urlBuilder.append(" ").append(interaction)
    }
    case Input(selector,content) => {
      driver.findElement(By.cssSelector(selector)).sendKeys(content)
      //      this.urlBuilder.append(" ").append(interaction)
    }
    case Select(selector,content) => {
      val element = driver.findElement(By.cssSelector(selector))
      val select = new ui.Select(element)
      select.selectByValue(content)
      //      this.urlBuilder.append(" ").append(interaction)
    }
    case _ => throw new UnsupportedOperationException
  }

  def getSnapshot: Page = new Page(driver.getCurrentUrl, driver.getPageSource)

  //  def getUrl: String = this.urlBuilder.mkString

  //remember to call this! don't want thousands of phantomJS browsers opened
  override def finalize = {
    driver.quit()
  }
}

object PageBuilder {

  def resolve(actions: Action*): Seq[(Seq[Interaction], Page, PageValues)] = {

    val results = ArrayBuffer[(Seq[Interaction],Page, PageValues)]()

    val pageValues = ArrayBuffer[PageValue]()
    val interactions = ArrayBuffer[Interaction]()

    val start_time = new Date().getTime

    val builder = new PageBuilder

    try {
      actions.foreach {
        action => action match {
          case interaction: Interaction => {
            builder.interact(interaction)
            interactions += interaction
            if (action.timer == true) pageValues += "timer" -> (new Date().getTime - start_time).toString
          }
          case snapshot: Snapshot => {
            if (snapshot.timer == true) pageValues += "timer" -> (new Date().getTime - start_time).toString
            if (snapshot.name != null) pageValues += "name" -> snapshot.name
            val page = builder.getSnapshot
            results.+=((interactions.clone().toSeq, page, pageValues.clone().toSeq))
          }
          case _ => throw new UnsupportedOperationException
        }
      }
    }
    finally
    {
      builder.finalize
    }

    return results.toSeq
  }

}