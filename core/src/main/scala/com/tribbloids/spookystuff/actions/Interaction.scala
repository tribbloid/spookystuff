package com.tribbloids.spookystuff.actions

import com.thoughtworks.selenium.SeleniumException
import org.openqa.selenium.interactions.{Actions => SeleniumActions}
import org.openqa.selenium.support.ui.{ExpectedCondition, ExpectedConditions, Select}
import org.openqa.selenium.{By, JavascriptExecutor, WebDriver}
import com.tribbloids.spookystuff.{SpookyContext, Const}
import com.tribbloids.spookystuff.actions.WaitForDocumentReady._
import com.tribbloids.spookystuff.row.PageRow
import com.tribbloids.spookystuff.expressions.{Expression, Literal}
import com.tribbloids.spookystuff.pages.{Page, Unstructured}
import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.utils.Utils

import scala.collection.mutable
import scala.concurrent.duration.Duration

/**
 * Interact with the browser (e.g. click a button or type into a search box) to reach the data page.
 * these will be logged into target page's backtrace.
 * failed interactive will trigger an error dump by snapshot.
 * has an option to be delayed to
 */
abstract class Interaction(
                            val delay: Duration,
                            val blocking: Boolean
                            ) extends Action {

  final override def outputNames = Set()

  final override def trunk = Some(this) //can't be ommitted

  final override def doExe(session: Session): Seq[Page] = {

    exeWithoutPage(session: Session)

    if (delay != null && delay.toMillis > 0) {
      Thread.sleep(delay.toMillis)
    }
    if (blocking) {
      driverWait(session).until(DocumentReadyCondition)
    }

    Nil
  }

  def exeWithoutPage(session: Session): Unit
}

object DocumentReadyCondition extends ExpectedCondition[Boolean] {

  val script = "return document.readyState"

  override def apply(input: WebDriver): Boolean = {

    val result = input match {
      case d: JavascriptExecutor => d.executeScript(script)
      case _ => throw new UnsupportedOperationException("this web browser driver is not supported")
    }

    result == "complete"
  }
}

/**
 * Type into browser's url bar and click "goto"
 * @param uri support cell interpolation
 */
case class Visit(
                  uri: Expression[Any],
                  override val delay: Duration = Const.interactionDelayMin,
                  override val blocking: Boolean = Const.interactionBlock
                  ) extends Interaction(delay, blocking) with Timed {

  override def exeWithoutPage(session: Session) {
    session.driver.get(uri.asInstanceOf[Literal[String]].value)

//    if (hasTitle) {
//      val wait = new WebDriverWait(session.driver, timeout(session).toSeconds)
//      wait.until(ExpectedConditions.not(ExpectedConditions.titleIs("")))
//    }
  }

  override def doInterpolate(pageRow: PageRow, spooky: SpookyContext): Option[this.type] = {
    val first = this.uri.lift(pageRow).flatMap(Utils.asArray[Any](_).headOption)

    val uriStr: Option[String] = first.flatMap {
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
 * @param delay seconds to be wait for
 */
case class Delay(
                  override val delay: Duration = Const.interactionDelayMin
                  ) extends Interaction(delay, false) with Driverless {

  override def exeWithoutPage(session: Session): Unit = {
    //do nothing
  }
}

/**
 * Wait for some random time, add some unpredictability
 * @param delay seconds to be wait for
 */
case class RandomDelay(
                        override val delay: Duration = Const.interactionDelayMin,
                        max: Duration = Const.interactionDelayMax
                        ) extends Interaction(delay, false) with Driverless {

  assert(max >= delay)

  override def exeWithoutPage(session: Session) {
    Thread.sleep(Utils.random.nextInt((max - delay).toMillis.toInt) )
  }
}

/**
 * Wait until at least one particular element appears, otherwise throws an exception
 * @param selector css selector of the element
 *              after which it will throw an exception!
 */
case class WaitFor(selector: String) extends Interaction(null, false) with Timed {

  override def exeWithoutPage(session: Session) {
    this.getElement(selector, session)
  }
}

case object WaitForDocumentReady extends Interaction(null, true) with Timed {

  override def exeWithoutPage(session: Session): Unit = {
    //do nothing
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
                  override val delay: Duration = Const.interactionDelayMin,
                  override val blocking: Boolean = Const.interactionBlock
                  ) extends Interaction(delay, blocking) with Timed {
  override def exeWithoutPage(session: Session) {
    val element = this.getClickableElement(selector, session)

    element.click()
  }
}

/**
 * Click an element with your mouse pointer.
 * @param selector css selector of the element, only the first element will be affected
 */
case class ClickNext(
                      selector: String,
                      exclude: Seq[String],
                      override val delay: Duration = Const.interactionDelayMin,
                      override val blocking: Boolean = Const.interactionBlock
                      ) extends Interaction(delay, blocking) with Timed {

  val clicked: mutable.HashSet[String] = mutable.HashSet(exclude: _*)

  override def exeWithoutPage(session: Session) {

    val elements = this.getElements(selector, session)

    import scala.collection.JavaConversions._

    elements.foreach{
      element => {
        if (!clicked.contains(element.getText)){
          driverWait(session).until(ExpectedConditions.elementToBeClickable(element))
          clicked += element.getText
          element.click()
          return
        }
      }
    }
    throw new SeleniumException("all elements has been clicked before")
  }

  override def doInterpolate(pageRow: PageRow, spooky: SpookyContext): Option[this.type] =
    Some(this.copy().asInstanceOf[this.type])
}

///**
// * Click an element with your mouse pointer.
// * @param selector css selector of the element, only the first element will be affected
// */
//case class ClickAll(
//                     selector: String
//                     )extends Interaction with Timed {
//
//  override def exeWithoutPage(session: Session) {
//
//    val elements = this.getElements(selector, session)
//
//    import scala.collection.JavaConversions._
//
//    elements.foreach{
//      element => {
//        driverWait(session).until(ExpectedConditions.elementToBeClickable(element))
//        element.click()
//      }
//    }
//  }
//}

/**
 * Submit a form, wait until new content returned by the submission has finished loading
 * @param selector css selector of the element, only the first element will be affected
 */
case class Submit(
                   selector: String,
                   override val delay: Duration = Const.interactionDelayMin,
                   override val blocking: Boolean = Const.interactionBlock
                   ) extends Interaction(delay, blocking) with Timed {
  override def exeWithoutPage(session: Session) {

    val element = this.getElement(selector, session)

    element.submit()
  }
}

/**
 * Type into a textbox
 * @param selector css selector of the textbox, only the first element will be affected
 * @param text support cell interpolation
 */
case class TextInput(
                      selector: String,
                      text: Expression[Any],
                      override val delay: Duration = Const.interactionDelayMin,
                      override val blocking: Boolean = Const.interactionBlock
                      ) extends Interaction(delay, blocking) with Timed {
  override def exeWithoutPage(session: Session) {

    val element = this.getElement(selector, session)

    element.sendKeys(text.asInstanceOf[Literal[String]].value)
  }

  override def doInterpolate(pageRow: PageRow, spooky: SpookyContext): Option[this.type] = {

    val first = this.text.lift(pageRow).flatMap(Utils.asArray[Any](_).headOption)

    val textStr: Option[String] = first.flatMap {
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
case class DropDownSelect(
                           selector: String,
                           value: Expression[Any],
                           override val delay: Duration = Const.interactionDelayMin,
                           override val blocking: Boolean = Const.interactionBlock
                           ) extends Interaction(delay, blocking) with Timed {
  override def exeWithoutPage(session: Session) {

    val element = this.getElement(selector, session)

    val select = new Select(element)
    select.selectByValue(value.asInstanceOf[Literal[String]].value)
  }

  override def doInterpolate(pageRow: PageRow, spooky: SpookyContext): Option[this.type] = {
    val first = this.value.lift(pageRow).flatMap(Utils.asArray[Any](_).headOption)

    val valueStr: Option[String] = first.flatMap {
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
case class SwitchToFrame(selector: String)extends Interaction(null, false) with Timed {
  override def exeWithoutPage(session: Session) {

    val element = this.getElement(selector, session)

    session.driver.switchTo().frame(element)
  }
}

/**
 * Execute a javascript snippet
 * @param script support cell interpolation
 * @param selector selector of the element this script is executed against, if null, against the entire page
 */
case class ExeScript(
                      script: Expression[Any],
                      selector: String = null,
                      override val delay: Duration = Const.interactionDelayMin,
                      override val blocking: Boolean = Const.interactionBlock
                      ) extends Interaction(delay, blocking) with Timed {
  override def exeWithoutPage(session: Session) {

    val element = if (selector == null) None
    else {
      val element = this.getElement(selector, session)
      Some(element)
    }

    val scriptStr = script.asInstanceOf[Literal[String]].value
    session.driver match {
      case d: JavascriptExecutor => d.executeScript(scriptStr, element.toArray: _*)
      case _ => throw new UnsupportedOperationException("this web browser driver is not supported")
    }
  }

  override def doInterpolate(pageRow: PageRow, spooky: SpookyContext): Option[this.type] = {
    val first = this.script.lift(pageRow).flatMap(Utils.asArray[Any](_).headOption)

    val scriptStr: Option[String] = first.flatMap {
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
                       handleSelector: String = "*",
                       override val delay: Duration = Const.interactionDelayMin,
                       override val blocking: Boolean = Const.interactionBlock
                       ) extends Interaction(delay, blocking) with Timed {

  override def exeWithoutPage(session: Session): Unit = {

    val element = this.getElement(selector, session)

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