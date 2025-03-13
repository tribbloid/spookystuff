package com.tribbloids.spookystuff.web.actions

import com.tribbloids.spookystuff.actions.{Interaction, RewriteRule, Trace}
import com.tribbloids.spookystuff.doc.{Doc, Unstructured}
import com.tribbloids.spookystuff.row.{AgentRow, SpookySchema}
import com.tribbloids.spookystuff.agent.Agent
import com.tribbloids.spookystuff.utils.SpookyUtils
import com.tribbloids.spookystuff.web.conf.Web
import com.tribbloids.spookystuff.{ActionException, Const}
import org.openqa.selenium.support.ui.{ExpectedCondition, ExpectedConditions, Select}
import org.openqa.selenium.{interactions, JavascriptExecutor, WebDriver}

import scala.collection.mutable
import scala.concurrent.duration.Duration

/**
  * Interact with the browser (e.g. click a button or type into a search box) to reach the data page. these will be
  * logged into target page's backtrace. failed interactive will trigger an error dump by snapshot. has an option to be
  * delayed to
  */
@SerialVersionUID(-6784287573066896999L)
abstract class WebInteraction(
    override val cooldown: Duration,
    val blocking: Boolean
) extends Interaction
    with WebDriverTimeout {

  import WebInteraction._

  override def doExe(agent: Agent): Seq[Doc] = {

    super.doExe(agent)

    if (blocking) {
      webDriverWait(agent).until(DocumentReadyCondition)
    }

    Nil
  }

  def webDriverActions(agent: Agent): interactions.Actions = {

    new org.openqa.selenium.interactions.Actions(agent.driverOf(Web))
  }
}

object WebInteraction {

  object DocumentReadyCondition extends ExpectedCondition[Boolean] {

    final def script: String = "return document.readyState"

    override def apply(input: WebDriver): Boolean = {

      val result = input match {
        case d: JavascriptExecutor => d.executeScript(script)
        case _ => throw new UnsupportedOperationException("this web browser driver is not supported")
      }

      result == "complete"
    }
  }
}

/**
  * Type into browser's url bar and click "goto"
  *
  * @param uri
  *   support cell interpolation
  */
case class Visit(
    uri: String,
    override val cooldown: Duration = Const.Interaction.delayMin,
    override val blocking: Boolean = Const.Interaction.blocking
) extends WebInteraction(cooldown, blocking) {

  override def exeNoOutput(agent: Agent): Unit = {
    agent.driverOf(Web).get(uri)

    //    if (hasTitle) {
    //      val wait = new WebDriverWait(session.driver, timeout(session).toSeconds)
    //      wait.until(ExpectedConditions.not(ExpectedConditions.titleIs("")))
    //    }
  }
}

/**
  * Wait until at least one particular element appears, otherwise throws an exception
  *
  * @param selector
  *   css selector of the element after which it will throw an exception!
  */
case class WaitFor(selector: Selector) extends WebInteraction(null, false) {

  override def exeNoOutput(agent: Agent): Unit = {
    this.getElement(selector, agent)
  }
}

case object WaitForDocumentReady extends WebInteraction(null, true) {

  override def exeNoOutput(agent: Agent): Unit = {
    // do nothing
  }
}

//experimental
//case class DelayForAjaxReady() extends Interaction {
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
  *
  * @param selector
  *   css selector of the element, only the first element will be affected
  */
case class Click(
    selector: Selector,
    override val cooldown: Duration = Const.Interaction.delayMin,
    override val blocking: Boolean = Const.Interaction.blocking
) extends WebInteraction(cooldown, blocking) {
  override def exeNoOutput(agent: Agent): Unit = {
    val element = this.getClickableElement(selector, agent)

    element.click()
  }
}

/**
  * Click an element with your mouse pointer.
  *
  * @param selector
  *   css selector of the element, only the first element will be affected
  */
case class ClickNext(
    selector: Selector,
    exclude: Seq[String],
    // TODO: remove this, and supercede with Selector
    override val cooldown: Duration = Const.Interaction.delayMin,
    override val blocking: Boolean = Const.Interaction.blocking
) extends WebInteraction(cooldown, blocking) {

  @transient lazy val clicked: mutable.HashSet[String] = mutable.HashSet(exclude: _*)

  override def exeNoOutput(agent: Agent): Unit = {

    val elements = this.getElements(selector, agent)

    import scala.jdk.CollectionConverters._

    elements.asScala.foreach { element =>
      {
        if (!clicked.contains(element.getText)) {
          webDriverWait(agent).until(ExpectedConditions.elementToBeClickable(element))
          clicked += element.getText
          element.click()
          return
        }
      }
    }
    throw new ActionException("all elements has been clicked before")
  }
}

/**
  * Submit a form, wait until new content returned by the submission has finished loading
  *
  * @param selector
  *   css selector of the element, only the first element will be affected
  */
case class Submit(
    selector: Selector,
    override val cooldown: Duration = Const.Interaction.delayMin,
    override val blocking: Boolean = Const.Interaction.blocking
) extends WebInteraction(cooldown, blocking) {
  override def exeNoOutput(agent: Agent): Unit = {

    val element = this.getElement(selector, agent)

    element.submit()
  }
}

/**
  * Type into a textbox
  *
  * @param selector
  *   css selector of the textbox, only the first element will be affected
  * @param text
  *   support cell interpolation
  */
case class TextInput(
    selector: Selector,
    text: String,
    override val cooldown: Duration = Const.Interaction.delayMin,
    override val blocking: Boolean = Const.Interaction.blocking
) extends WebInteraction(cooldown, blocking) {
  override def exeNoOutput(agent: Agent): Unit = {

    val element = this.getElement(selector, agent)

    element.sendKeys(text)
  }
}

/**
  * Select an item from a drop down list
  *
  * @param selector
  *   css selector of the drop down list, only the first element will be affected
  * @param value
  *   support cell interpolation
  */
case class DropDownSelect(
    selector: Selector,
    value: String,
    override val cooldown: Duration = Const.Interaction.delayMin,
    override val blocking: Boolean = Const.Interaction.blocking
) extends WebInteraction(cooldown, blocking) {
  override def exeNoOutput(agent: Agent): Unit = {

    val element = this.getElement(selector, agent)

    val select = new Select(element)
    select.selectByValue(value)
  }
}

/**
  * Request browser to change focus to a frame/iframe embedded in the global page, after which only elements inside the
  * focused frame/iframe can be selected. Can be used multiple times to switch focus back and forth
  *
  * @param selector
  *   css selector of the frame/iframe, only the first element will be affected
  */
//TODO: not possible to switch back, need a better abstraction
case class ToFrame(selector: Selector) extends WebInteraction(null, false) {
  override def exeNoOutput(agent: Agent): Unit = {

    val element = this.getElement(selector, agent)

    agent.driverOf(Web).switchTo().frame(element)
  }
}

/**
  * Execute a javascript snippet
  *
  * @param script
  *   support cell interpolation
  * @param selector
  *   selector of the element this script is executed against, if null, against the entire page
  */
case class ExeScript(
    script: String,
    selector: Selector = null,
    override val cooldown: Duration = Const.Interaction.delayMin,
    override val blocking: Boolean = Const.Interaction.blocking
) extends WebInteraction(cooldown, blocking) {
  override def exeNoOutput(agent: Agent): Unit = {

    val element =
      if (selector == null) None
      else {
        val element = this.getElement(selector, agent)
        Some(element)
      }

    agent.driverOf(Web) match {
      case d: JavascriptExecutor => d.executeScript(script, element.toArray: _*)
      case _                     => throw new UnsupportedOperationException("this web browser driver is not supported")
    }
  }
}

/**
  * @param selector
  *   selector of the slide bar
  * @param percentage
  *   distance and direction of moving of the slider handle, positive number is up/right, negative number is down/left
  * @param handleSelector
  *   selector of the slider
  */
case class DragSlider(
    selector: Selector,
    percentage: Double,
    handleSelector: Selector = "*",
    override val cooldown: Duration = Const.Interaction.delayMin,
    override val blocking: Boolean = Const.Interaction.blocking
) extends WebInteraction(cooldown, blocking) {

  override def exeNoOutput(agent: Agent): Unit = {

    val element = this.getElement(selector, agent)

    val handle = element.findElement(handleSelector.by)

    val dim = element.getSize
    val height = dim.getHeight
    val width = dim.getWidth

    webDriverActions(agent).clickAndHold(handle).perform()

    Thread.sleep(1000)

    webDriverActions(agent).moveByOffset(1, 0).perform()

    Thread.sleep(1000)

    if (width > height)
      webDriverActions(agent)
        .moveByOffset((width * percentage).asInstanceOf[Int], 0)
        .perform()
    else
      webDriverActions(agent)
        .moveByOffset(0, (height * percentage).asInstanceOf[Int])
        .perform()

    Thread.sleep(1000)

    webDriverActions(agent).release().perform()
  }
}
