package com.tribbloids.spookystuff.session

import org.openqa.selenium.WebDriver

import scala.language.implicitConversions

object CleanWebDriver {

  implicit def unwrap(v: CleanWebDriver): WebDriver = v.self
}

class CleanWebDriver(
                      val self: WebDriver,
                      override val lifespan: Lifespan = Lifespan.Auto()
                    ) extends LocalCleanable {

  override def cleanImpl(): Unit = {
    self.close()
    self.quit()
  }
}
