package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.conf.Web
import com.tribbloids.spookystuff.session.Session
import org.openqa.selenium.WebElement
import org.openqa.selenium.support.ui.{ExpectedConditions, WebDriverWait}

import java.util

trait WebTimed extends WebAction with Timed {

  def webDriverWait(session: Session): WebDriverWait =
    new WebDriverWait(session.driverOf(Web), this.timeout(session).max.toSeconds)

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
