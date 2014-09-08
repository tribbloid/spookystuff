package org.tribbloid.spookystuff.entity

import java.net._
import java.util.Date
import javax.net.ssl.{HttpsURLConnection, SSLContext, TrustManager}

import org.apache.commons.io.IOUtils
import org.apache.http.conn.ssl.AllowAllHostnameVerifier
import org.openqa.selenium.By
import org.openqa.selenium.htmlunit.HtmlUnitDriver
import org.openqa.selenium.interactions.Actions
import org.openqa.selenium.remote.RemoteWebDriver
import org.openqa.selenium.support.events.EventFiringWebDriver
import org.openqa.selenium.support.ui.{ExpectedConditions, Select, WebDriverWait}
import org.tribbloid.spookystuff.{Utils, Const}
import org.tribbloid.spookystuff.factory.PageBuilder
import org.tribbloid.spookystuff.utils._

import scala.collection.mutable.ArrayBuffer

/**
 * Created by peng on 04/06/14.
 */

object ClientAction {

  def interpolate[T](str: String, map: collection.Map[String,T]): String = {
    if ((str == null)|| str.isEmpty) return str
    else if ((map == null)|| map.isEmpty) return str
    var strVar = str
    for (entry <- map) {
      val sub = "#{".concat(entry._1).concat("}")
      if (strVar.contains(sub))
      {
        var value: String = "null"
        if (entry._2 != null) {value = entry._2.toString}
        strVar = strVar.replace(sub, value)
      }
    }
    strVar
  }

  def snapshotNotOmitted(actions: ClientAction*): Boolean = {
    if (actions.isEmpty) {
      false
    }
    else {
      for (action <- actions) {
        action match {
          case a: Export => return true
          case a: Container =>
            if (a.snapshotOmitted) return true
          case a: Interactive =>
          case _ =>
        }
      }
      false
    }
  }
}

/**
 * These are the same actions a human would do to get to the data page,
 * their order of execution is identical to that they are defined.
 * Many supports **Cell Interpolation**: you can embed cell reference in their constructor
 * by inserting keys enclosed by `#{}`, in execution they will be replaced with values they map to.
 * This is used almost exclusively in typing into a textbox, but it's flexible enough to be used anywhere.
 */
trait ClientAction extends Serializable with Cloneable {

  var timeline: Long = -1 //this should be a val

  override def clone(): AnyRef = super.clone()

  private var canFail: Boolean = false //TODO: change to trait?

  def canFail(value: Boolean = true): this.type = {
    this.canFail = value
    this
  }

  def interpolate[T](map: Map[String,T]): this.type = this

  final def exe(pb: PageBuilder): Array[Page] = {

    try {
      val pages = this match {
        case d: Timed =>
          Utils.withDeadline(d.timeout) {
            doExe(pb: PageBuilder)
          }
        case _ => doExe(pb: PageBuilder)
      }

      val newTimeline = new Date().getTime - pb.start_time

      if (this.isInstanceOf[Interactive]) {
        val cloned = this.clone().asInstanceOf[Interactive] //TODO: EVIL! CHANGE TO copy if possible
        cloned.timeline = newTimeline
        pb.backtrace.add(cloned)
      }

      //      if (this.isInstanceOf[Export]) {
      //        pages = pages.map(page => page.copy(alias = this.asInstanceOf[Export].alias))
      //      }

      pages
    }
    catch {
      case e: Throwable =>

        if (this.isInstanceOf[Interactive]) {

          val page = Snapshot().exe(pb).toList(0)
          try {
            page.save(pb.spooky.errorDumpPath(page))(pb.spooky.hConf)
          }
          catch {
            case e: Throwable =>
              page.saveLocal(pb.spooky.localErrorDumpPath(page))
          }
          // TODO: logError("error Page saved as "+errorFileName)
        }

        if (!canFail) {
          throw e
        }
        else {
          null
        }
    }
  }

  def doExe(pb: PageBuilder): Array[Page]
}

trait Timed extends ClientAction {

  val timeout: Int = Const.resourceTimeout
}

/**
 * Interact with the browser (e.g. click a button or type into a search box) to reach the data page.
 * these will be logged into target page's backtrace.
 * failed interactive will trigger an error dump by snapshot.
 */
trait Interactive extends ClientAction {

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
trait Sessionless extends ClientAction {

  override final def doExe(pb: PageBuilder): Array[Page] = this.exeWithoutSession

  def exeWithoutSession: Array[Page]
}

/**
 * Only for complex workflow control,
 * each defines a nested/non-linear subroutine that may or may not be executed
 * once or multiple times depending on situations.
 */
trait Container extends ClientAction {
  //  override val timeout: Int = Int.MaxValue

  def snapshotOmitted: Boolean
}

/**
 * Export a page from the browser or http client
 * the page an be anything including HTML/XML file, image, PDF file or JSON string.
 */
trait Export extends ClientAction {
  var alias: String = null

  def as(alias: String): this.type = { //TODO: better way to return type?
    this.alias = alias
    this
  }
}

/**
 * Type into browser's url bar and click "goto"
 * @param url support cell interpolation
 */
case class Visit(url: String) extends Interactive with Timed {
  override def exeWithoutResult(pb: PageBuilder) {
    pb.driver.get(url)
  }

  override def interpolate[T](map: Map[String,T]): this.type = {
    Visit(ClientAction.interpolate(this.url,map)).asInstanceOf[this.type]
  }
}

/**
 * Wait for some time
 * @param delay seconds to be wait for
 */
case class Delay(delay: Int = Const.pageDelay) extends Interactive {
  //  override val timeout = Math.max(Const.driverCallTimeout, delay + 10)

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
case class DelayFor(selector: String, delay: Int = Const.pageDelay) extends Interactive {
  //  override val timeout = Math.max(Const.driverCallTimeout, delay + 10)

  override def exeWithoutResult(pb: PageBuilder) {
    val wait = new WebDriverWait(pb.driver, delay)
    wait.until(ExpectedConditions.presenceOfElementLocated(By.cssSelector(selector)))
  }
}

/**
 * Click an element with your mouse pointer.
 * @param selector css selector of the element, only the first element will be affected
 */
case class Click(selector: String, delay: Int = Const.pageDelay) extends Interactive {
  override def exeWithoutResult(pb: PageBuilder) {
    val wait = new WebDriverWait(pb.driver, delay)
    val element = wait.until(ExpectedConditions.visibilityOfElementLocated(By.cssSelector(selector)))

    element.click()
  }
}

/**
 * Submit a form, wait until new content returned by the submission has finished loading
 * @param selector css selector of the element, only the first element will be affected
 */
case class Submit(selector: String, delay: Int = Const.pageDelay) extends Interactive {
  override def exeWithoutResult(pb: PageBuilder) {
    val wait = new WebDriverWait(pb.driver, delay)
    val element = wait.until(ExpectedConditions.presenceOfElementLocated(By.cssSelector(selector)))

    element.submit()
  }
}

/**
 * Type into a textbox
 * @param selector css selector of the textbox, only the first element will be affected
 * @param text support cell interpolation
 */
case class TextInput(selector: String, text: String, delay: Int = Const.pageDelay) extends Interactive {
  override def exeWithoutResult(pb: PageBuilder) {
    val wait = new WebDriverWait(pb.driver, delay)
    val element = wait.until(ExpectedConditions.presenceOfElementLocated(By.cssSelector(selector)))

    element.sendKeys(text)
  }

  override def interpolate[T](map: Map[String,T]): this.type = {
    TextInput(this.selector, ClientAction.interpolate(this.text,map)).asInstanceOf[this.type]
  }
}

/**
 * Select an item from a drop down list
 * @param selector css selector of the drop down list, only the first element will be affected
 * @param text support cell interpolation
 */
case class DropDownSelect(selector: String, text: String, delay: Int = Const.pageDelay) extends Interactive {
  override def exeWithoutResult(pb: PageBuilder) {
    val wait = new WebDriverWait(pb.driver, delay)
    val element = wait.until(ExpectedConditions.presenceOfElementLocated(By.cssSelector(selector)))

    val select = new Select(element)
    select.selectByValue(text)
  }

  override def interpolate[T](map: Map[String,T]): this.type = {
    DropDownSelect(this.selector, ClientAction.interpolate(this.text,map)).asInstanceOf[this.type]
  }
}

/**
 * Request browser to change focus to a frame/iframe embedded in the global page,
 * after which only elements inside the focused frame/iframe can be selected.
 * Can be used multiple times to switch focus back and forth
 * @param selector css selector of the frame/iframe, only the first element will be affected
 */
case class SwitchToFrame(selector: String, delay: Int = Const.pageDelay) extends Interactive {
  override def exeWithoutResult(pb: PageBuilder) {
    val wait = new WebDriverWait(pb.driver, delay)
    val element = wait.until(ExpectedConditions.presenceOfElementLocated(By.cssSelector(selector)))

    pb.driver.switchTo().frame(element)
  }
}

/**
 * Execute a javascript snippet
 * @param script support cell interpolation
 */
case class ExeScript(
                      script: String,
                      override val timeout: Int = Const.resourceTimeout
                      ) extends Interactive with Timed {
  override def exeWithoutResult(pb: PageBuilder) {
    pb.driver match {
      case d: HtmlUnitDriver => d.executeScript(script)
      //      case d: AndroidWebDriver => d.executeScript(script)
      case d: EventFiringWebDriver => d.executeScript(script)
      case d: RemoteWebDriver => d.executeScript(script)
      case _ => throw new UnsupportedOperationException("this web browser driver is not supported")
    }
  }

  override def interpolate[T](map: Map[String,T]): this.type = {
    ExeScript(ClientAction.interpolate(this.script,map)).asInstanceOf[this.type]
  }
}

/**
 *
 * @param selector selector of the slide bar
 * @param percentage distance and direction of moving of the slider handle, positive number is up/right, negative number is down/left
 * @param handleSelector selector of the slider
 */
case class DragSlider(
                       selector: String,
                       percentage: Double,
                       delay: Int = Const.pageDelay,
                       handleSelector: String = "*"
                       )
  extends Interactive {

  override def exeWithoutResult(pb: PageBuilder): Unit = {

    val wait = new WebDriverWait(pb.driver, delay)
    val element = wait.until(ExpectedConditions.presenceOfElementLocated(By.cssSelector(selector)))

    val slider = element.findElement(By.cssSelector(handleSelector))

    val dim = element.getSize
    val height = dim.getHeight
    val width = dim.getWidth

    val move = new Actions(pb.driver)

    if (width > height){
      move.dragAndDropBy(slider, (width*percentage).asInstanceOf[Int], 0).build().perform()
    }
    else {
      move.dragAndDropBy(slider, 0, (height*percentage).asInstanceOf[Int]).build().perform()
    }
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

    Array[Page](page.copy(backtrace = backtrace))
  }
}

/**
 * use an http GET to fetch a remote resource deonted by url
 * http client is much faster than browser, also load much less resources
 * recommended for most static pages.
 * actions for more complex http/restful API call will be added per request.
 * @param url support cell interpolation
 */
case class Wget(url: String) extends Export with Sessionless{

  override def exeWithoutSession: Array[Page] = {
    if ((url == null)|| url.isEmpty) return Array[Page]()

    CookieHandler.setDefault(new CookieManager(null, CookiePolicy.ACCEPT_ALL))

    val uc: URLConnection =  new URL(url).openConnection()
    uc.setConnectTimeout(Const.resourceTimeout*1000)
    uc.setReadTimeout(Const.resourceTimeout*1000)

    uc match {
      case huc: HttpsURLConnection =>
        // Install the all-trusting trust manager
        val sslContext = SSLContext.getInstance( "SSL" )
        sslContext.init(null, Array[TrustManager](new InsecureTrustManager()), null)
        // Create an ssl socket factory with our all-trusting manager
        val sslSocketFactory  = sslContext.getSocketFactory

        huc.setSSLSocketFactory(sslSocketFactory)
        huc.setHostnameVerifier(new AllowAllHostnameVerifier)

      case _ =>
    }

    uc.connect()
    val is = uc.getInputStream

    val content = IOUtils.toByteArray(is)

    is.close()

    Array[Page](
      new Page(url,
        content,
        contentType = uc.getContentType
      ) //will not export backtrace right now
    )
  }

  override def interpolate[T](map: Map[String,T]): this.type = {
    Wget(ClientAction.interpolate(this.url,map)).asInstanceOf[this.type]
  }
}

/**
 * Contains several sub-actions that are iterated for multiple times
 * Will iterate until max iteration is reached or execution is impossible (sub-action throws an exception)
 * @param times max iteration, default to Const.fetchLimit
 * @param actions a list of actions being iterated through
 */
case class Loop(times: Int = Const.fetchLimit)(val actions: ClientAction*) extends Container {

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
      case e: Throwable =>
      //Do nothing, loop until conditions are not met
    }

    results.toArray
  }

  override def snapshotOmitted: Boolean = ClientAction.snapshotNotOmitted(actions: _*)

  override def interpolate[T](map: Map[String,T]): this.type = {

    Loop(times)(actions.map(_.interpolate(map)): _*).asInstanceOf[this.type ]
  }
}

//case class If(selector: String)(exist: ClientAction*)(notExist: ClientAction*) extends Container {
//
//}