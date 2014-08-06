package org.tribbloid.spookystuff.entity

import java.io.Serializable
import java.net.{URL, URLConnection}
import java.util
import java.util.Date
import javax.net.ssl.{HttpsURLConnection, SSLContext, TrustManager}

import org.apache.commons.io.IOUtils
import org.apache.http.conn.ssl.AllowAllHostnameVerifier
import org.openqa.selenium.By
import org.openqa.selenium.htmlunit.HtmlUnitDriver
import org.openqa.selenium.remote.RemoteWebDriver
import org.openqa.selenium.support.events.EventFiringWebDriver
import org.openqa.selenium.support.ui
import org.tribbloid.spookystuff.Conf
import org.tribbloid.spookystuff.factory.PageBuilder
import org.tribbloid.spookystuff.utils.InsecureTrustManager
import org.tribbloid.spookystuff.utils.withDeadline

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
 * Created by peng on 04/06/14.
 */

object ActionUtils {
  //TODO: reverse the direction of look-up, if a '#{...}' has no corresponding key in the context, throws an exception
  def formatWithContext[T](str: String, context: util.Map[String,T]): String = {
    if ((str == null)||(str.isEmpty)) return str
    else if ((context == null)||(context.isEmpty)) return str
    var strVar = str
    for (entry <- context) {
      val sub = "#{".concat(entry._1).concat("}")
      if (strVar.contains(sub))
      {
        var value: String = "null"
        if (entry._2 != null) {value = entry._2.toString}
        //      TODO:  if (value.matches("[^#{}]+") == false) throw new UnsupportedOperationException("context value cannot contain #{} etc.")
        strVar = strVar.replace(sub, value)
      }
    }
    strVar
  }

  def mayHaveResult(actions: Action*): Boolean = {
    for (action <- actions) {
      action match {
        case a: Export => return true
        case a: Container => {
          if (a.mayHaveResult == true) return true
        }
        case a: Interactive =>
        case _ =>
      }
    }
    return false
  }
}

/**
 * These are the same actions a human would do to get to the data page,
 * their order of execution is identical to that they are defined.
 * Many supports **Context Interpolation**: you can embed context reference in their constructor
 * by inserting context's keys enclosed by `#{}`, in execution they will be replaced with values they map to.
 * This is used almost exclusively in typing into a textbox, but it's flexible enough to be used anywhere.
 */
trait Action extends Serializable with Cloneable {

  var timeline: Long = -1

  def format[T](context: util.Map[String,T]): this.type = this

  override def clone(): AnyRef = super.clone()

  final def exe(pb: PageBuilder): Array[Page] = {

    try {
      var pages = withDeadline(Conf.driverCallTimeout) {
        doExe(pb: PageBuilder)
      }

      val newTimeline = new Date().getTime - pb.start_time

      if (this.isInstanceOf[Interactive]) {
        val cloned = this.clone().asInstanceOf[Interactive] //TODO: EVIL!
        cloned.timeline = newTimeline
        pb.backtrace.add(cloned)
      }

      if (this.isInstanceOf[Export]) {
        pages = pages.map(page => page.copy(alias = this.asInstanceOf[Export].alias))
      }

      return pages
    }
    catch {
      case e: Throwable => {

        if (this.isInstanceOf[Interactive]) {

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

        throw e
      }
    }
  }

  def doExe(pb: PageBuilder): Array[Page]
}

/**
 * Interact with the browser (e.g. click a button or type into a search box) to reach the data page.
 * these will be logged into target page's backtrace.
 * failed interactive will trigger an error dump by snapshot.
 */
trait Interactive extends Action {

  override final def doExe(pb: PageBuilder): Array[Page] = {
    exeWithoutResult(pb)
    null
  }

  def exeWithoutResult(pb: PageBuilder): Unit
}

/**
 * Http client operations that doesn't require a browser
 * e.g. wget, restful API invocation
 */
trait Sessionless extends Action {

  override final def doExe(pb: PageBuilder): Array[Page] = this.exeWithoutSession

  def exeWithoutSession: Array[Page]
}

/**
 * Only for complex workflow control,
 * each defines a nested/non-linear subroutine that may or may not be executed
 * once or multiple times depending on situations.
 */
trait Container extends Action {
  def mayHaveResult: Boolean
}

/**
 * Export a page from the browser or http client
 * the page an be anything including HTML/XML file, image, PDF file or JSON string.
 */
trait Export extends Action {
  var alias: String = null

  def as(alias: String): this.type = { //TODO: better way to return type?
    this.alias = alias
    return this
  }
}

/**
 * Type into browser's url bar and click "goto"
 * @param url support context interpolation
 */
case class Visit(val url: String) extends Interactive {
  override def exeWithoutResult(pb: PageBuilder) {
    pb.driver.get(url)
  }

  override def format[T](context: util.Map[String,T]): this.type = {
    Visit(ActionUtils.formatWithContext(this.url,context)).asInstanceOf[this.type]
  }
}

/**
 * Wait for some time
 * @param delay seconds to be wait for
 */
case class Delay(val delay: Int = Conf.pageDelay) extends Interactive {
  override def exeWithoutResult(pb: PageBuilder) {
    Thread.sleep(delay * 1000)
  }
}

/**
 * Wait until at least one particular element appears, otherwise throws an exception
 * @param selector css selector of the element
 * @param delay maximum waiting time in seconds,
 *              after which it will throw an exception!
 */
case class DelayFor(val selector: String,val delay: Int = Conf.pageDelay) extends Interactive {
  override def exeWithoutResult(pb: PageBuilder) {
    val wait = new ui.WebDriverWait(pb.driver, delay)
    wait.until(ui.ExpectedConditions.presenceOfAllElementsLocatedBy(By.cssSelector(selector)))
  }
}

/**
 * Click an element with your mouse pointer.
 * @param selector css selector of the element, only the first element will be affected
 */
case class Click(val selector: String) extends Interactive {
  override def exeWithoutResult(pb: PageBuilder) {
    pb.driver.findElement(By.cssSelector(selector)).click()
  }
}

/**
 * Submit a form, wait until new content returned by the submission has finished loading
 * @param selector css selector of the element, only the first element will be affected
 */
case class Submit(val selector: String) extends Interactive {
  override def exeWithoutResult(pb: PageBuilder) {
    pb.driver.findElement(By.cssSelector(selector)).submit()
  }
}

/**
 * Type into a textbox
 * @param selector css selector of the textbox, only the first element will be affected
 * @param text support context interpolation
 */
case class TextInput(val selector: String, val text: String) extends Interactive {
  override def exeWithoutResult(pb: PageBuilder) {
    pb.driver.findElement(By.cssSelector(selector)).sendKeys(text)
  }

  override def format[T](context: util.Map[String,T]): this.type = {
    TextInput(this.selector, ActionUtils.formatWithContext(this.text,context)).asInstanceOf[this.type]
  }
}

/**
 * Select an item from a drop down list
 * @param selector css selector of the drop down list, only the first element will be affected
 * @param text support context interpolation
 */
case class DropDownSelect(val selector: String, val text: String) extends Interactive {
  override def exeWithoutResult(pb: PageBuilder) {
    val element = pb.driver.findElement(By.cssSelector(selector))
    val select = new ui.Select(element)
    select.selectByValue(text)
  }

  override def format[T](context: util.Map[String,T]): this.type = {
    DropDownSelect(this.selector, ActionUtils.formatWithContext(this.text,context)).asInstanceOf[this.type]
  }
}

/**
 * Request browser to change focus to a frame/iframe embedded in the global page,
 * after which only elements inside the focused frame/iframe can be selected.
 * Can be used multiple times to switch focus back and forth
 * @param selector css selector of the frame/iframe, only the first element will be affected
 */
case class SwitchToFrame(val selector: String) extends Interactive {
  override def exeWithoutResult(pb: PageBuilder) {
    val element = pb.driver.findElement(By.cssSelector(selector))
    pb.driver.switchTo().frame(element)
  }
}

/**
 * Execute a javascript snippet
 * @param script support context interpolation
 */
case class ExeScript(val script: String) extends Interactive {
  override def exeWithoutResult(pb: PageBuilder) {
    pb.driver match {
      case d: HtmlUnitDriver => d.executeScript(script)
//      case d: AndroidWebDriver => d.executeScript(script)
      case d: EventFiringWebDriver => d.executeScript(script)
      case d: RemoteWebDriver => d.executeScript(script)
      case _ => throw new UnsupportedOperationException("this web browser driver is not supported")
    }
  }

  override def format[T](context: util.Map[String,T]): this.type = {
    ExeScript(ActionUtils.formatWithContext(this.script,context)).asInstanceOf[this.type]
  }
}

/**
 * Export the current page from the browser
 * interact with the browser to load the target page first
 * only for html page, please use wget for images and pdf files
 * always export as UTF8 charset
 */
case class Snapshot() extends Export {
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

/**
 * use an http GET to fetch a remote resource deonted by url
 * http client is much faster than browser, also load much less resources
 * recommended for most static pages.
 * actions for more complex http/restful API call will be added per request.
 * @param url support context interpolation
 */
case class Wget(val url: String) extends Export with Sessionless{

  override def exeWithoutSession(): Array[Page] = {
    if ((url == null)||(url.isEmpty)) return Array[Page](PageBuilder.emptyPage)

    val uc: URLConnection =  new URL(url).openConnection()

    uc match {
      case huc: HttpsURLConnection => {
        // Install the all-trusting trust manager
        val sslContext = SSLContext.getInstance( "SSL" )
        sslContext.init(null, Array[TrustManager](new InsecureTrustManager()), null)
        // Create an ssl socket factory with our all-trusting manager
        val sslSocketFactory  = sslContext.getSocketFactory();

        huc.setSSLSocketFactory(sslSocketFactory)
        huc.setHostnameVerifier(new AllowAllHostnameVerifier)
      }

      case _ => {}
    }

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
    Wget(ActionUtils.formatWithContext(this.url,context)).asInstanceOf[this.type]
  }
}

/**
 * Contains several sub-actions that are iterated for multiple times
 * Will iterate until max iteration is reached or execution is impossible (sub-action throws an exception)
 * @param times max iteration, default to Conf.fetchLimit
 * @param actions a list of actions being iterated through
 */
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

  override def mayHaveResult: Boolean = ActionUtils.mayHaveResult(actions: _*)
}

//case class If(selector: String)(exist: Action*)(notExist: Action*) extends Container {
//
//}