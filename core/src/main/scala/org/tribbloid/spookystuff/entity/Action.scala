package org.tribbloid.spookystuff.entity

import org.tribbloid.spookystuff.Conf

import sys.process._
import java.util
import org.openqa.selenium.{By, WebDriver}
import org.openqa.selenium.support.ui
import java.io.File
import java.io.Serializable
import java.net.URL
import scala.collection.JavaConversions._

/**
 * Created by peng on 04/06/14.
 */

private object Action {
  def formatWithContext[T](str: String, context: util.Map[String,T]): String = {
    if ((context == null)||(context.isEmpty)) return str
    var strVar = str
    context.foreach {
      pair => {
        val sub = "#{".concat(pair._1).concat("}")
        if (strVar.contains(sub))
        {
          val value = pair._2.toString
          if (value.matches("[^#{}]+") == false) throw new UnsupportedOperationException("context value cannot contain #{} etc.")
          strVar = strVar.replace(sub, value)
        }
      }
    }
    strVar
  }
}

abstract class Action extends Serializable {
  var timeline: Long = -1

  def format[T](context: util.Map[String,T]): this.type = this
  //  def setTimer(on: Boolean = true) = { this.timer = on}
}

//TODO: Seriously, I don't know how to use these fancy things with case class & pattern matching
//trait Aliased {
//  val alias: String = null
//}

//represents an action that potentially changes a page
//TODO: considering nested structure for maximum control
//these will be added into page's backtrace
abstract class Interaction extends Action {
  def exe(driver: WebDriver): Unit

}

// these will yield a page
abstract class Extraction() extends Action {
  var alias: String = null

  def exe(driver: WebDriver): HtmlPage

  def as(alias: String): this.type = { //TODO: better way to return type?
    this.alias = alias
    return this
  }
}

//these will do neither of the above
abstract class Dump extends Action {
  def exe(driver: WebDriver): Unit
}

case class Visit(val url: String) extends Interaction{
  override def exe(driver: WebDriver) {
    driver.get(url)
  }

  override def format[T](context: util.Map[String,T]): this.type = {
    Visit(Action.formatWithContext(this.url,context)).asInstanceOf[this.type]
  }
}

case class Delay(val delay: Int = Conf.pageDelay) extends Interaction{
  override def exe(driver: WebDriver) {
    Thread.sleep(delay * 1000)
  }
}

//CAUTION: will throw an exception if the element doesn't appear in time!
case class DelayFor(val selector: String,val delay: Int) extends Interaction{
  override def exe(driver: WebDriver) {
    val wait = new ui.WebDriverWait(driver, delay)
    wait.until(ui.ExpectedConditions.presenceOfAllElementsLocatedBy(By.cssSelector(selector)))
  }
}

case class Click(val selector: String) extends Interaction{
  override def exe(driver: WebDriver) {
    driver.findElement(By.cssSelector(selector)).click()
  }
}

case class Submit(val selector: String) extends Interaction{
  override def exe(driver: WebDriver) {
    driver.findElement(By.cssSelector(selector)).submit()
  }
}

case class TextInput(val selector: String, val text: String) extends Interaction{
  override def exe(driver: WebDriver) {
    driver.findElement(By.cssSelector(selector)).sendKeys(text)
  }

  override def format[T](context: util.Map[String,T]): this.type = {
    TextInput(this.selector, Action.formatWithContext(this.text,context)).asInstanceOf[this.type]
  }
}

case class Select(val selector: String, val text: String) extends Interaction{
  override def exe(driver: WebDriver) {
    val element = driver.findElement(By.cssSelector(selector))
    val select = new ui.Select(element)
    select.selectByValue(text)
  }

  override def format[T](context: util.Map[String,T]): this.type = {
    Select(this.selector, Action.formatWithContext(this.text,context)).asInstanceOf[this.type]
  }
}

case class Snapshot() extends Extraction{
  // all other fields are empty
  override def exe(driver: WebDriver): HtmlPage = {
    new HtmlPage(driver.getCurrentUrl, driver.getPageSource, alias = this.alias)
  }
}

case class Wget(val url: String, val name: String, val path: String = Conf.savePagePath) extends Dump{

  //TODO: buggy may cause fileNotFoundExcepiton
  override def exe(driver: WebDriver) {
    val dir: File = new File(path)
    if (!dir.isDirectory) dir.mkdirs()

    val file: File = new File(path, name)
    if (!file.exists) file.createNewFile();

    new URL(url) #> file !!
  }
}

//case class Screenshot(val name: String = null) extends Extraction //screenshot feature is disabled
//case class GetText(val selector: String) extends Dump
//case class GetLink(val selector: String) extends Dump
//case class GetSrc(val selector: String) extends Dump
//case class GetAttr(val selector: String, val attr: String) extends Dump