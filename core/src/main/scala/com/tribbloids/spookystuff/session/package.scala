package com.tribbloids.spookystuff

import org.openqa.selenium.WebDriver

/**
 * Created by peng on 12/1/14.
 */
package object session {

  type CleanWebDriver = WebDriver with CleanWebDriverHelper
}
