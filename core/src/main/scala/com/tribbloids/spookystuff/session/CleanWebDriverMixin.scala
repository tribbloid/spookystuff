package com.tribbloids.spookystuff.session

import org.openqa.selenium.{NoSuchSessionException, WebDriver}
import org.slf4j.LoggerFactory

/**
 *
 */
object Cleanable {

}

trait Cleanable {

  def clean(): Unit

  override def finalize(): Unit = {
    try {
      clean()
    }
    catch {
      case e: NoSuchSessionException => //already cleaned before
      case e: Throwable =>
        LoggerFactory.getLogger(this.getClass).warn(s"!!!!! FAIL TO CLEAN UP ${this.getClass.getSimpleName} !!!!!"+e)
    }
    finally {
      super.finalize()
    }
  }
}


trait CleanWebDriverMixin extends Cleanable {
  this: WebDriver =>

  def clean(): Unit = {
    this.close()
    this.quit()
  }
}