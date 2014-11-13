package org.tribbloid.spookystuff.factory

import org.openqa.selenium.WebDriver
import org.slf4j.LoggerFactory

/**
 * Created by peng on 11/5/14.
 */

trait CleanWebDriverFinalize {
  self: WebDriver =>

  override def finalize(): Unit = {
    try {
      this.close()
      this.quit()
    }
    catch {
      case e: Throwable =>
        LoggerFactory.getLogger(this.getClass).warn("!!!!!FAIL TO CLEANED UP DRIVER!!!!!")
        throw e
    }
    finally {
      super.finalize()
    }
  }
}