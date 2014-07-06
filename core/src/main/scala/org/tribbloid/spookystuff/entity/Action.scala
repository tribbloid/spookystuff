package org.tribbloid.spookystuff.entity

import java.io.Serializable
import java.net.{URL, URLConnection}
import java.util
import java.util.Date

import org.apache.commons.io.IOUtils
import org.openqa.selenium.By
import org.openqa.selenium.support.ui
import org.tribbloid.spookystuff.Conf

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
 * Created by peng on 04/06/14.
 */

private object ActionUtils {
  //TODO: reverse the direction of look-up, if a '#{...}' has no corresponding key in the context, throws an exception
  def formatWithContext[T](str: String, context: util.Map[String,T]): String = {
    if ((context == null)||(context.isEmpty)) return str
    var strVar = str
    for (entry <- context) {
      val sub = "#{".concat(entry._1).concat("}")
      if (strVar.contains(sub))
      {
        val value = entry._2.toString
        //      TODO:  if (value.matches("[^#{}]+") == false) throw new UnsupportedOperationException("context value cannot contain #{} etc.")
        strVar = strVar.replace(sub, value)
      }
    }
    strVar
  }

}

trait Action extends Serializable with Cloneable {

  var timeline: Long = -1

  def format[T](context: util.Map[String,T]): this.type = this

  override def clone(): AnyRef = super.clone()

  final def exe(pb: PageBuilder): Array[Page] = {

    try {
      var pages = doExe(pb: PageBuilder)
      var newTimeline = new Date().getTime - pb.start_time

      if (this.isInstanceOf[Interactive]) {
        val cloned = this.clone().asInstanceOf[Interactive] //TODO: EVIL!
        cloned.timeline = newTimeline
        pb.backtrace.add(cloned)
      }

      if (this.isInstanceOf[Aliased]) {
        pages = pages.map(page => page.copy(alias = this.asInstanceOf[Aliased].alias))
      }

      return pages
    }
    catch {
      case e: Throwable => {

        if (this.isInstanceOf[ErrorDump]) {

          val page = Snapshot().exe(pb).toList(0)
          //          try {
          //            page.save(dir = Conf.errorPageDumpDir)
          //          }
          //          catch {
          //            case e: Throwable => {
          page.saveLocal(dir = Conf.localErrorPageDumpDir)
          //            }
          //          }
          //                  TODO: logError("Error Page saved as "+errorFileName)
        }

        throw e //try to delegate all failover to Spark, but this may change in the future
      }
    }
  }

  def doExe(pb: PageBuilder): Array[Page]
}
//represents an action that potentially changes a page in a browser
//these will be logged into page's backtrace
trait Interactive extends Action

trait ErrorDump extends Action

trait Sessionless extends Action

trait Container extends Action

trait Aliased extends Action {
  var alias: String = null

  def as(alias: String): this.type = { //TODO: better way to return type?
    this.alias = alias
    return this
  }
}

case class Visit(val url: String) extends Interactive with ErrorDump{
  override def doExe(pb: PageBuilder): Array[Page] = {
    pb.driver.get(url)
    return null
  }

  override def format[T](context: util.Map[String,T]): this.type = {
    Visit(ActionUtils.formatWithContext(this.url,context)).asInstanceOf[this.type]
  }
}


case class Delay(val delay: Int = Conf.pageDelay) extends Interactive with ErrorDump{
  override def doExe(pb: PageBuilder): Array[Page] = {
    Thread.sleep(delay * 1000)
    return null
  }
}

//CAUTION: will throw an exception if the element doesn't appear in time!
case class DelayFor(val selector: String,val delay: Int) extends Interactive with ErrorDump{
  override def doExe(pb: PageBuilder): Array[Page] = {
    val wait = new ui.WebDriverWait(pb.driver, delay)
    wait.until(ui.ExpectedConditions.presenceOfAllElementsLocatedBy(By.cssSelector(selector)))
    return null
  }
}

case class Click(val selector: String) extends Interactive with ErrorDump{
  override def doExe(pb: PageBuilder): Array[Page] = {
    pb.driver.findElement(By.cssSelector(selector)).click()
    return null
  }
}

case class Submit(val selector: String) extends Interactive with ErrorDump{
  override def doExe(pb: PageBuilder): Array[Page] = {
    pb.driver.findElement(By.cssSelector(selector)).submit()
    return null
  }
}

case class TextInput(val selector: String, val text: String) extends Interactive with ErrorDump{
  override def doExe(pb: PageBuilder): Array[Page] = {
    pb.driver.findElement(By.cssSelector(selector)).sendKeys(text)
    return null
  }

  override def format[T](context: util.Map[String,T]): this.type = {
    TextInput(this.selector, ActionUtils.formatWithContext(this.text,context)).asInstanceOf[this.type]
  }
}

case class Select(val selector: String, val text: String) extends Interactive with ErrorDump{
  override def doExe(pb: PageBuilder): Array[Page] = {
    val element = pb.driver.findElement(By.cssSelector(selector))
    val select = new ui.Select(element)
    select.selectByValue(text)
    return null
  }

  override def format[T](context: util.Map[String,T]): this.type = {
    Select(this.selector, ActionUtils.formatWithContext(this.text,context)).asInstanceOf[this.type]
  }
}

case class Snapshot() extends Aliased {
  // all other fields are empty
  override def doExe(pb: PageBuilder): Array[Page] = {
    val page =       new Page(
      pb.driver.getCurrentUrl,
      pb.driver.getPageSource.getBytes("UTF8"),
      contentType = "text/html; charset=UTF-8"
    )

    val backtrace = pb.backtrace.toArray(new Array[Interactive](pb.backtrace.size()))

    return Array[Page](page.copy(backtrace = backtrace))
  }
}

case class Wget(val url: String) extends Aliased with Sessionless{

  override def doExe(pb: PageBuilder): Array[Page] = {
    val uc: URLConnection =  new URL(url).openConnection()

    uc.connect()
    val is = uc.getInputStream()

    val content = IOUtils.toByteArray(is)

    is.close()

    Array[Page](
      new Page(url,
        content,
        contentType = uc.getContentType
      ) //will not export backtrace right now
    )
  }

  override def format[T](context: util.Map[String,T]): this.type = {
    Wget(ActionUtils.formatWithContext(this.url,context)).asInstanceOf[this.type] //TODO: ugly tail
  }
}

case class Loop(val times: Int = Conf.fetchLimit)(val actions: Action*) extends Container {

  override def doExe(pb: PageBuilder): Array[Page] = {

    val results = new ArrayBuffer[Page]()

    try {
      for (i <- 0 until times) {
        for (action <- actions) {
          val pages = action.exe(pb)
          if (pages != null) results.++=(pages)
        }
      }
    }
    catch {
      case e: Throwable => {
        //Do nothing, loop until conditions are not met
      }
    }

    return results.toArray
  }
}

//case class If(selector: String)(exist: Action*)(notExist: Action*) extends Container {
//
//}