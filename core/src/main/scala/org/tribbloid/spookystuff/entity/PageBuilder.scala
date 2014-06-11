package org.tribbloid.spookystuff.entity

import java.util.Date
import org.tribbloid.spookystuff.conf.Conf

import org.openqa.selenium.phantomjs.PhantomJSDriver
import org.openqa.selenium.By
import org.openqa.selenium.support.ui
import scala.collection.mutable.ArrayBuffer

import org.openqa.selenium.support.ui.{ExpectedConditions, WebDriverWait}
import com.google.common.collect.ArrayListMultimap
import sys.process._
import java.io.File
import java.net.URL
import org.apache.spark.Logging

object PageBuilder {

  def resolve(actions: Action*): Seq[(Seq[Interaction], PageWithValues)] = {

    val results = ArrayBuffer[(Seq[Interaction],PageWithValues)]()

    val builder = new PageBuilder

    try {
      actions.foreach {
        action => action match {
          case ii: Interaction => {
            builder.interact(ii)
          }
          case ee: Extraction => {
            results += builder.traceInteractions -> builder.extract(ee)
          }
          case dd: Dump => {
            builder.dump(dd)
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

//TODO: need refactoring to accept more openqa drivers
private class PageBuilder extends Logging {

  private val driver = new PhantomJSDriver(Conf.phantomJSCaps);
  private val start_time = new Date().getTime
  private val pageValues: ArrayListMultimap[String,String] = ArrayListMultimap.create()
  private val interactions = ArrayBuffer[Interaction]()

  //by default driver should be reset in this case, but whatever

  //try to delegate all failover to Spark, but this may change in the future
  def interact(interaction: Interaction) = {

    try {
      interactions += interaction
      interaction match {
        case Visit(url) => {
          driver.get(url)
        }
        case Delay(delay) => {
          Thread.sleep(delay * 1000)
        }
        case DelayFor(selector, delay) => {
          val wait = new WebDriverWait(driver, delay)
          wait.until(ExpectedConditions.presenceOfAllElementsLocatedBy(By.cssSelector(selector)))
        }
        //TODO: still need nullPointerException handling!
        case Click(selector) => {
          driver.findElement(By.cssSelector(selector)).click()
        }
        case Submit(selector) => {
          driver.findElement(By.cssSelector(selector)).submit()
        }
        case Input(selector, content) => {
          driver.findElement(By.cssSelector(selector)).sendKeys(content)
        }
        case Select(selector, content) => {
          val element = driver.findElement(By.cssSelector(selector))
          val select = new ui.Select(element)
          select.selectByValue(content)
        }
        case _ => throw new UnsupportedOperationException
      }
      if (interaction.timer == true) pageValues.put("timer", (new Date().getTime - start_time).toString)
    }
    catch {
      case e: Throwable => {
        val pageWithValues = this.extract(Snapshot())
        pageWithValues.page.save(pageWithValues.hashCode().toString+".error")
        logError("Error Page saved as "+pageWithValues.hashCode().toString+".error")
        throw e
      }
    }
  }

  def extract(extraction: Extraction): PageWithValues = {

    var content: String = null //TODO: not simple

    extraction match {
      case Snapshot() => {
        content = driver.getPageSource
      }
    }

    val page = new Page(driver.getCurrentUrl, content)

    if (extraction.timer == true) pageValues.put("timer", (new Date().getTime - start_time).toString)
    val resultValues: ArrayListMultimap[String, String] = ArrayListMultimap.create(pageValues)
    PageWithValues(page, resultValues)
  }

  def dump(dump: Dump) = dump match {
    case Wget(url, name, path) => {

      val dir: File = new File(path)
      if (!dir.isDirectory) dir.mkdirs()

      val file: File = new File(path, name)
      if (!file.exists) file.createNewFile();

      new URL(url) #> file !!
    }
    case Insert(key, value) => {
      pageValues.put(key,value)
    }
    case GetText(selector) => {
      pageValues.put(selector, driver.findElement(By.cssSelector(selector)).getText)
    }
    case GetLink(selector) => {
      pageValues.put(selector, driver.findElement(By.cssSelector(selector)).getAttribute("href"))
    }
    case GetSrc(selector) => {
      pageValues.put(selector, driver.findElement(By.cssSelector(selector)).getAttribute("src"))
    }
    case GetAttr(selector, attr) => {
      pageValues.put(selector, driver.findElement(By.cssSelector(selector)).getAttribute(attr))
    }// will export info that are not on the current interacted page, but what the hell
      if (dump.timer == true) pageValues.put("timer", (new Date().getTime - start_time).toString)
  }

  def traceInteractions: Seq[Interaction] = this.interactions.clone().toSeq
  //  def getUrl: String = this.urlBuilder.mkString

  //remember to call this! don't want thousands of phantomJS browsers opened
  override def finalize = {
    driver.quit()
  }
}