package org.tribbloid.spookystuff

import org.openqa.selenium.WebDriver

import scala.language.implicitConversions

/**
 * Created by peng on 11/5/14.
 */
package object factory {

  type CleanWebDriver = WebDriver with CleanWebDriverFinalize

  implicit def cleanWebDriver(webDriver: WebDriver): CleanWebDriver = webDriver.asInstanceOf[CleanWebDriver]
}
