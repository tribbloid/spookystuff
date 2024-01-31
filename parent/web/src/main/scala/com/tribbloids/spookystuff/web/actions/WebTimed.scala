package com.tribbloids.spookystuff.web.actions

import com.tribbloids.spookystuff.actions.Timed
import com.tribbloids.spookystuff.session.Agent
import com.tribbloids.spookystuff.web.conf.Web
import org.openqa.selenium.WebElement
import org.openqa.selenium.support.ui.{ExpectedConditions, WebDriverWait}

import java.time.Duration
import java.util

import scala.language.implicitConversions

trait WebTimed extends WebAction with Timed.ThreadUnsafe {

  implicit def nanos2JDuration(v: Long): Duration = java.time.Duration.ofNanos(v)

  def webDriverWait(agent: Agent): WebDriverWait =
    new WebDriverWait(agent.driverOf(Web), this.timeout(agent).max.toNanos)

  def getClickableElement(selector: Selector, agent: Agent): WebElement = {

    val elements = webDriverWait(agent).until(ExpectedConditions.elementToBeClickable(selector.by))

    elements
  }

  def getElement(selector: Selector, agent: Agent): WebElement = {

    val elements = webDriverWait(agent).until(ExpectedConditions.presenceOfElementLocated(selector.by))

    elements
  }

  def getElements(selector: Selector, agent: Agent): util.List[WebElement] = {

    val elements = webDriverWait(agent).until(ExpectedConditions.presenceOfAllElementsLocatedBy(selector.by))

    elements
  }
}
