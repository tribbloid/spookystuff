package org.tribbloid.spookystuff.entity

import java.util.Date
import org.tribbloid.spookystuff.conf.Conf
import org.openqa.selenium.phantomjs.PhantomJSDriver
import org.openqa.selenium.By
import org.openqa.selenium.support.ui
import org.jsoup.Jsoup
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
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

  def firstLink(selector: String): String = doc.select(selector).first().attr("href")

  def allLinks(selector: String): Seq[String] = {
    val result = ArrayBuffer[String]()

    doc.select(selector).foreach{
      element => result += element.attr("href")
    }

    return result.toSeq
  }

  def firstText(selector: String): String = doc.select(selector).first().text

  def allTexts(selector: String): Seq[String] = {
    val result = ArrayBuffer[String]()

    doc.select(selector).foreach{
      element => result += element.text
    }

    return result.toSeq
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
    case Wait(seconds) => {
      wait(seconds*1000)
      //      this.urlBuilder.append(" ").append(interaction)
    }
    case Click(selector, repeat) => {
      driver.findElement(By.cssSelector(selector)).click()
      //      this.urlBuilder.append(" ").append(interaction)
    }
    case Submit(selector, repeat) => {
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
}

object PageBuilder {

  def resolve(actions: Action*): Seq[(Seq[Interaction], Page, PageValues)] = {
    val results = ArrayBuffer[(Seq[Interaction],Page, PageValues)]()

    val builder = new PageBuilder
    val pageValues = ArrayBuffer[PageValue]()
    val interactions = ArrayBuffer[Interaction]()

    val start_time = new Date().getTime

    actions.foreach {
      action => action match {
        case interaction: Interaction => {
          builder.interact(interaction)
          interactions += interaction
          if (action.timer==true) pageValues += "timer" -> (new Date().getTime - start_time).toString
        }
        case snapshot: Snapshot => {
          if (snapshot.timer==true) pageValues += "timer" -> (new Date().getTime - start_time).toString
          if (snapshot.name!=null) pageValues += "name" -> snapshot.name
          val page = builder.getSnapshot
          results.+= (( interactions.clone().toSeq, page ,pageValues.clone().toSeq))
        }
        case _ => throw new UnsupportedOperationException
      }
    }

    return results.toSeq
  }

}