package com.tribbloids.spookystuff.session

import com.tribbloids.spookystuff.utils.NOTSerializable
import org.openqa.selenium.WebDriver

import scala.language.implicitConversions

object CleanWebDriver {

  implicit def unwrap(v: CleanWebDriver): WebDriver = v.self
}

class CleanWebDriver(
                      val self: WebDriver,
                      override val lifespan: Lifespan = new Lifespan.Auto()
                    ) extends LocalCleanable {

  def cleanImpl(): Unit = {
    self.close()
    self.quit()
  }
}
