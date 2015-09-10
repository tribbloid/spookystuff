package com.tribbloids.spookystuff.session

import org.openqa.selenium.WebDriver
import org.openqa.selenium.remote.SessionNotFoundException
import org.slf4j.LoggerFactory

/**
 * Created by peng on 11/5/14.
 */

trait CleanWebDriverHelper {
  this: WebDriver =>

  override def finalize(): Unit = {
    try {
      this.close()
      this.quit()
    }
    catch {
      case e: SessionNotFoundException => //already cleaned before
      case e: Throwable =>
        LoggerFactory.getLogger(this.getClass).warn("!!!!!FAIL TO CLEANE UP DRIVER!!!!!"+e)
    }
    finally {
      super.finalize()
    }
  }
}