package org.tribbloid.spookystuff.actions

import com.thoughtworks.selenium.SeleniumException
import org.openqa.selenium.htmlunit.HtmlUnitDriver
import org.openqa.selenium.interactions.{Actions => SeleniumActions}
import org.openqa.selenium.remote.RemoteWebDriver
import org.openqa.selenium.support.events.EventFiringWebDriver
import org.openqa.selenium.support.ui.{ExpectedCondition, ExpectedConditions, Select, WebDriverWait}
import org.openqa.selenium.{By, WebDriver}
import org.tribbloid.spookystuff.Const
import org.tribbloid.spookystuff.entity.PageRow
import org.tribbloid.spookystuff.expressions.{Expr, Literal}
import org.tribbloid.spookystuff.pages.{Page, Unstructured}
import org.tribbloid.spookystuff.session.Session
import org.tribbloid.spookystuff.utils.Utils

import scala.collection.mutable
import scala.concurrent.duration.Duration

/**
 * Interact with the browser (e.g. click a button or type into a search box) to reach the data page.
 * these will be logged into target page's backtrace.
 * failed interactive will trigger an error dump by snapshot.
 * has an option to be delayed to
 */
abstract class Interaction extends Action {

  final override def outputNames = Set()

  final override def trunk = Some(this) //can't be ommitted

  final override def doExe(session: Session): Seq[Page] = {

    exeWithoutPage(session: Session)

    Seq()
  }

  def exeWithoutPage(session: Session): Unit
}

/**
 * Type into browser's url bar and click "goto"
 * @param uri support cell interpolation
 */
case class Visit(
                  uri: Expr[Any],
                  hasTitle: Boolean = true
                  ) extends Interaction with Timed {

  override def exeWithoutPage(session: Session) {
    session.driver.get(uri.asInstanceOf[Literal[String]].value)

    if (hasTitle) {
      val wait = new WebDriverWait(session.driver, timeout(session).toSeconds)
      wait.until(ExpectedConditions.not(ExpectedConditions.titleIs("")))
    }
  }

  override def doInterpolate(pageRow: PageRow): Option[this.type] = {
    val uriStr: Option[String] = this.uri(pageRow).flatMap {
      case element: Unstructured => element.href
      case str: String => Option(str)
      case obj: Any => Option(obj.toString)
      case other => None
    }

    uriStr.map(
      str =>
        this.copy(uri = new Literal(str)).asInstanceOf[this.type]
    )
  }
}

/**
 * Wait for some time
 * @param min seconds to be wait for
 */
case class Delay(min: Duration = Const.actionDelayMax) extends Interaction {
  //  override val timeout = Math.max(Const.driverCallTimeout, delay + 10)

  override def exeWithoutPage(session: Session) {
    Thread.sleep(min.toMillis)
  }
}

/**
 * Wait for some random time, add some unpredictability
 * @param min seconds to be wait for
 */
case class RandomDelay(
                        min: Duration = Const.actionDelayMin,
                        max: Duration = Const.actionDelayMax
                        ) extends Interaction {

  assert(max >= min)

  //  override val timeout = Math.max(Const.driverCallTimeout, delay + 10)

  override def exeWithoutPage(session: Session) {
    Thread.sleep(min.toMillis + Utils.random.nextInt((max - min).toMillis.toInt) )
  }
}

/**
 * Wait until at least one particular element appears, otherwise throws an exception
 * @param selector css selector of the element
 *              after which it will throw an exception!
 */
case class WaitFor(selector: String) extends Interaction with Timed {
  //  override val timeout = Math.max(Const.driverCallTimeout, delay + 10)

  override def exeWithoutPage(session: Session) {
    val wait = new WebDriverWait(session.driver, timeout(session).toSeconds)
    wait.until(ExpectedConditions.presenceOfElementLocated(By.cssSelector(selector)))
  }
}

case object WaitForDocumentReady extends Interaction with Timed {

  object DocumentReadyCondition extends ExpectedCondition[Boolean] {

    override def apply(input: WebDriver): Boolean = {

      val script = "return document.readyState"

      val result = input match {
        case d: HtmlUnitDriver => d.executeScript(script)
        //      case d: AndroidWebDriver => d.executeScript(script)
        case d: EventFiringWebDriver => d.executeScript(script)
        case d: RemoteWebDriver => d.executeScript(script)
        case _ => throw new UnsupportedOperationException("this web browser driver is not supported")
      }

      result == "complete"
    }
  }

  override def exeWithoutPage(session: Session): Unit = {
    val wait = new WebDriverWait(session.driver, timeout(session).toSeconds)

    wait.until(DocumentReadyCondition)
  }
}


//experimental
//case class DelayForAjaxReady() extends Interaction with Timed {
//
//  object AjaxReadyCondition extends ExpectedCondition[Boolean] {
//
//    override def apply(input: WebDriver): Boolean = {
//
//      val jQueryScript = "jQuery.active"
//      val prototypeScript = "Ajax.activeRequestCount"
//      val dojoScript = "dojo.io.XMLHTTPTransport.inFlight.length"
//
//      input match {
//        case d: HtmlUnitDriver => d.executeScript(jQueryScript) == 0||d.executeScript(prototypeScript) == 0||d.executeScript(dojoScript) == 0
//        //      case d: AndroidWebDriver => d.executeScript(jQueryScript) == 0||d.executeScript(prototypeScript) == 0||d.executeScript(dojoScript) == 0
//        case d: EventFiringWebDriver => d.executeScript(jQueryScript) == 0||d.executeScript(prototypeScript) == 0||d.executeScript(dojoScript) == 0
//        case d: RemoteWebDriver => d.executeScript(jQueryScript) == 0||d.executeScript(prototypeScript) == 0||d.executeScript(dojoScript) == 0
//        case _ => throw new UnsupportedOperationException("this web browser driver is not supported")
//      }
//    }
//  }
//
//  override def exeWithoutPage(session: PageBuilder): Unit = {
//    val wait = new WebDriverWait(session.driver, delay.toSeconds)
//
//    wait.until(AjaxReadyCondition)
//  }
//}

/**
 * Click an element with your mouse pointer.
 * @param selector css selector of the element, only the first element will be affected
 */
case class Click(
                  selector: String,
                  clickable: Boolean = true //TODO: probably useless in most cases
                  )extends Interaction with Timed {
  override def exeWithoutPage(session: Session) {
    val wait = new WebDriverWait(session.driver, timeout(session).toSeconds)
    val element = if (clickable) wait.until(ExpectedConditions.elementToBeClickable(By.cssSelector(selector)))
    else wait.until(ExpectedConditions.presenceOfElementLocated(By.cssSelector(selector)))

    element.click()
  }
}

/**
 * Click an element with your mouse pointer.
 * @param selector css selector of the element, only the first element will be affected
 */
case class ClickNext(
                      selector: String,
                      exclude: Seq[String]
                      )extends Interaction with Timed {

  val clicked: mutable.HashSet[String] = mutable.HashSet(exclude: _*)

  override def exeWithoutPage(session: Session) {
    val wait = new WebDriverWait(session.driver, timeout(session).toSeconds)
    val elements = wait.until(ExpectedConditions.presenceOfAllElementsLocatedBy(By.cssSelector(selector)))

    import scala.collection.JavaConversions._

    elements.foreach{
      element => {
        wait.until(ExpectedConditions.elementToBeClickable(element))
        if (!clicked.contains(element.getText)){
          element.click()
          clicked += element.getText
          return
        }
      }
    }
    throw new SeleniumException("all elements has been clicked before")
  }

  override def doInterpolate(pageRow: PageRow): Option[this.type] =
    Some(this.copy().asInstanceOf[this.type])
}

/**
 * Click an element with your mouse pointer.
 * @param selector css selector of the element, only the first element will be affected
 */
case class ClickAll(
                     selector: String
                     )extends Interaction with Timed {

  override def exeWithoutPage(session: Session) {
    val wait = new WebDriverWait(session.driver, timeout(session).toSeconds)
    val elements = wait.until(ExpectedConditions.presenceOfAllElementsLocatedBy(By.cssSelector(selector)))

    import scala.collection.JavaConversions._

    elements.foreach{
      element => {
        wait.until(ExpectedConditions.elementToBeClickable(element))
        element.click()
      }
    }
  }
}

/**
 * Submit a form, wait until new content returned by the submission has finished loading
 * @param selector css selector of the element, only the first element will be affected
 */
case class Submit(selector: String) extends Interaction with Timed {
  override def exeWithoutPage(session: Session) {
    val wait = new WebDriverWait(session.driver, timeout(session).toSeconds)
    val element = wait.until(ExpectedConditions.presenceOfElementLocated(By.cssSelector(selector)))

    element.submit()
  }
}

/**
 * Type into a textbox
 * @param selector css selector of the textbox, only the first element will be affected
 * @param text support cell interpolation
 */
case class TextInput(selector: String, text: Expr[Any]) extends Interaction with Timed {
  override def exeWithoutPage(session: Session) {
    val wait = new WebDriverWait(session.driver, timeout(session).toSeconds)
    val element = wait.until(ExpectedConditions.presenceOfElementLocated(By.cssSelector(selector)))

    element.sendKeys(text.asInstanceOf[Literal[String]].value)
  }

  override def doInterpolate(pageRow: PageRow): Option[this.type] = {
    val textStr: Option[String] = this.text(pageRow).flatMap {
      case element: Unstructured => element.text
      case str: String => Option(str)
      case obj: Any => Option(obj.toString)
      case other => None
    }

    textStr.map(
      str =>
        this.copy(text = new Literal(str)).asInstanceOf[this.type]
    )
  }
}

/**
 * Select an item from a drop down list
 * @param selector css selector of the drop down list, only the first element will be affected
 * @param value support cell interpolation
 */
case class DropDownSelect(selector: String, value: Expr[Any]) extends Interaction with Timed {
  override def exeWithoutPage(session: Session) {
    val wait = new WebDriverWait(session.driver, timeout(session).toSeconds)
    val element = wait.until(ExpectedConditions.presenceOfElementLocated(By.cssSelector(selector)))

    val select = new Select(element)
    select.selectByValue(value.asInstanceOf[Literal[String]].value)
  }

  override def doInterpolate(pageRow: PageRow): Option[this.type] = {
    val valueStr: Option[String] = this.value(pageRow).flatMap {
      case element: Unstructured => element.attr("value")
      case str: String => Option(str)
      case obj: Any => Option(obj.toString)
      case other => None
    }

    valueStr.map(
      str =>
        this.copy(value = new Literal(str)).asInstanceOf[this.type]
    )
  }
}

/**
 * Request browser to change focus to a frame/iframe embedded in the global page,
 * after which only elements inside the focused frame/iframe can be selected.
 * Can be used multiple times to switch focus back and forth
 * @param selector css selector of the frame/iframe, only the first element will be affected
 */
case class SwitchToFrame(selector: String)extends Interaction with Timed {
  override def exeWithoutPage(session: Session) {
    val wait = new WebDriverWait(session.driver, timeout(session).toSeconds)
    val element = wait.until(ExpectedConditions.presenceOfElementLocated(By.cssSelector(selector)))

    session.driver.switchTo().frame(element)
  }
}

/**
 * Execute a javascript snippet
 * @param script support cell interpolation
 * @param selector selector of the element this script is executed against, if null, against the entire page
 */
case class ExeScript(script: Expr[Any], selector: String = null) extends Interaction with Timed {
  override def exeWithoutPage(session: Session) {

    val element = if (selector == null) None
    else {
      val wait = new WebDriverWait(session.driver, timeout(session).toSeconds)
      val result = wait.until(ExpectedConditions.presenceOfElementLocated(By.cssSelector(selector)))
      Some(result)
    }

    val scriptStr = script.asInstanceOf[Literal[String]].value
    session.driver match {
      case d: HtmlUnitDriver => d.executeScript(scriptStr, element.toArray: _*)//scala can't cast directly
      //      case d: AndroidWebDriver => throw new UnsupportedOperationException("this web browser driver is not supported")
      case d: EventFiringWebDriver => d.executeScript(scriptStr, element.toArray: _*)
      case d: RemoteWebDriver => d.executeScript(scriptStr, element.toArray: _*)
      case _ => throw new UnsupportedOperationException("this web browser driver is not supported")
    }
  }

  override def doInterpolate(pageRow: PageRow): Option[this.type] = {
    val scriptStr: Option[String] = this.script(pageRow).flatMap {
      case element: Unstructured => element.text
      case str: String => Option(str)
      case obj: Any => Option(obj.toString)
      case other => None
    }

    scriptStr.map(
      str =>
        this.copy(script = new Literal(str)).asInstanceOf[this.type]
    )
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
                       handleSelector: String = "*"
                       )
  extends Interaction with Timed {

  override def exeWithoutPage(session: Session): Unit = {

    val wait = new WebDriverWait(session.driver, timeout(session).toSeconds)
    //    val element = wait.until(ExpectedConditions.elementToBeClickable(By.cssSelector(selector)))
    val element = wait.until(ExpectedConditions.visibilityOfElementLocated(By.cssSelector(selector)))

    val handle = element.findElement(By.cssSelector(handleSelector))

    val dim = element.getSize
    val height = dim.getHeight
    val width = dim.getWidth

    new SeleniumActions(session.driver).clickAndHold(handle).perform()

    Thread.sleep(1000)

    new SeleniumActions(session.driver).moveByOffset(1, 0).perform()

    Thread.sleep(1000)

    if (width > height) new SeleniumActions(session.driver).moveByOffset((width * percentage).asInstanceOf[Int], 0).perform()
    else new SeleniumActions(session.driver).moveByOffset(0, (height * percentage).asInstanceOf[Int]).perform()

    Thread.sleep(1000)

    new SeleniumActions(session.driver).release().perform()
  }
}