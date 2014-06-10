package org.tribbloid.spookystuff.entity

import java.util.Date
import org.tribbloid.spookystuff.conf.Conf

import org.openqa.selenium.phantomjs.PhantomJSDriver
import org.openqa.selenium.By
import org.openqa.selenium.support.ui
import scala.collection.mutable.ArrayBuffer

import org.openqa.selenium.support.ui.{ExpectedConditions, WebDriverWait}
import com.google.common.collect.ArrayListMultimap

object PageBuilder {

  def resolve(actions: Action*): Seq[(Seq[Interaction], PageWithValues)] = {

    val results = ArrayBuffer[(Seq[Interaction],PageWithValues)]()

    val builder = new PageBuilder

    try {
      actions.foreach {
        action => action match {
          case interaction: Interaction => {
            builder.interact(interaction)
          }
          case snapshot: Snapshot => {
            results += builder.traceInteractions -> builder.snapshot(snapshot)
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
private class PageBuilder {

  private val driver = new PhantomJSDriver(Conf.phantomJSCaps);
  private val start_time = new Date().getTime
  private val pageValues: ArrayListMultimap[String,String] = ArrayListMultimap.create()
  private val interactions = ArrayBuffer[Interaction]()

  //by default driver should be reset in this case, but whatever

  //try to delegate all failover to Spark, but this may change in the future
  def interact(interaction: Interaction) = {

    interactions += interaction
    interaction match {
      case Visit(url) => {
        driver.get(url)
      }
      case Delay(delay) => {
        Thread.sleep(delay*1000)
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
      case Input(selector,content) => {
        driver.findElement(By.cssSelector(selector)).sendKeys(content)
      }
      case Select(selector,content) => {
        val element = driver.findElement(By.cssSelector(selector))
        val select = new ui.Select(element)
        select.selectByValue(content)
      }
      case _ => throw new UnsupportedOperationException
    }
    if (interaction.timer == true) pageValues.put("timer",(new Date().getTime - start_time).toString)
  }

  def snapshot(extraction: Snapshot): PageWithValues = {
    val resultValues: ArrayListMultimap[String,String] = ArrayListMultimap.create(pageValues)

    if (extraction.name != null) resultValues.put("name", extraction.name)
    val page = new Page(driver.getCurrentUrl, driver.getPageSource)
    val result = PageWithValues(page, resultValues)

    if (extraction.timer == true) pageValues.put("timer",(new Date().getTime - start_time).toString)
    result
  }

  def traceInteractions: Seq[Interaction] = this.interactions.clone().toSeq

  //  def getUrl: String = this.urlBuilder.mkString

  //remember to call this! don't want thousands of phantomJS browsers opened
  override def finalize = {
    driver.quit()
  }
}