package com.tribbloids.spookystuff.session

import com.tribbloids.spookystuff.utils.lifespan.Lifespan
import org.openqa.selenium.{NoSuchSessionException, WebDriver}

import scala.language.implicitConversions

object CleanWebDriver {

  implicit def unwrap(v: CleanWebDriver): WebDriver = v.self
}

class CleanWebDriver(
    val self: WebDriver,
    override val _lifespan: Lifespan = Lifespan.TaskOrJVM()
) extends DriverLike {

  override def cleanImpl(): Unit = {
    self.close()
    self.quit()
  }

  override def silentOnError(ee: Throwable): Boolean = {
    ee match {
      case _: NoSuchSessionException => true
      case _                         => false
    }
  }
}
