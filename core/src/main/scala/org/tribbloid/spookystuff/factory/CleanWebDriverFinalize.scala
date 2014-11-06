package org.tribbloid.spookystuff.factory

import org.openqa.selenium.WebDriver

/**
 * Created by peng on 11/5/14.
 */
trait CleanWebDriverFinalize {
  self: WebDriver =>

  override def finalize(): Unit = {
    this.close()
    this.quit()
  }
}