package com.tribbloids.spookystuff.web.actions

import com.tribbloids.spookystuff.actions.Interaction
import com.tribbloids.spookystuff.agent.Agent
import com.tribbloids.spookystuff.doc.Doc
import com.tribbloids.spookystuff.web.conf.Web
import org.openqa.selenium.support.ui.ExpectedCondition
import org.openqa.selenium.{interactions, JavascriptExecutor, WebDriver}

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
    with WebAction
    with BrowserTimeout {

  import WebInteraction.*

  override def doExe(agent: Agent): Seq[Doc] = {

    super.doExe(agent)

    if (blocking) {
      webDriverWait(agent).until(DocumentReadyCondition)
    }

    Nil
  }

  def webDriverActions(agent: Agent): interactions.Actions = {

    new org.openqa.selenium.interactions.Actions(agent.getDriver(Web))
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
