package com.tribbloids.spookystuff.web.actions

import com.tribbloids.spookystuff.actions.Timed
import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.web.conf.Web
import org.openqa.selenium.WebElement
import org.openqa.selenium.support.ui.{ExpectedConditions, WebDriverWait}

import java.time.Duration
import java.util

import scala.language.implicitConversions

trait WebTimed extends WebAction with Timed.ThreadUnsafe {

  implicit def nanos2JDuration(v: Long): Duration = java.time.Duration.ofNanos(v)

  def webDriverWait(session: Session): WebDriverWait =
    new WebDriverWait(session.driverOf(Web), this.timeout(session).max.toNanos)

  def getClickableElement(selector: Selector, session: Session): WebElement = {

    val elements = webDriverWait(session).until(ExpectedConditions.elementToBeClickable(selector.by))

    elements
  }

  def getElement(selector: Selector, session: Session): WebElement = {

    val elements = webDriverWait(session).until(ExpectedConditions.presenceOfElementLocated(selector.by))

    elements
  }

  def getElements(selector: Selector, session: Session): util.List[WebElement] = {

    val elements = webDriverWait(session).until(ExpectedConditions.presenceOfAllElementsLocatedBy(selector.by))

    elements
  }
}
