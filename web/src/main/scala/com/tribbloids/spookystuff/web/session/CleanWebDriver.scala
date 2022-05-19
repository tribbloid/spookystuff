package com.tribbloids.spookystuff.web.session

import com.tribbloids.spookystuff.session.DriverLike
import com.tribbloids.spookystuff.utils.lifespan.Cleanable.Lifespan
import com.tribbloids.spookystuff.utils.{CommonConst, CommonUtils}
import org.openqa.selenium.{NoSuchSessionException, WebDriver}
import org.slf4j.LoggerFactory

import java.net.ConnectException
import scala.language.implicitConversions

object CleanWebDriver {

  implicit def unwrap(v: CleanWebDriver): WebDriver = v.self
}

class CleanWebDriver(
    val self: WebDriver,
    override val _lifespan: Lifespan = Lifespan.TaskOrJVM().forShipping
) extends DriverLike {

  override def cleanImpl(): Unit = {
    try {
      CommonUtils.retry(CommonConst.driverClosingRetries) {
        CommonUtils.withTimeout(CommonConst.driverClosingTimeout) {

          self.close()
        }
        Thread.sleep(1000)

      }
    } catch {
      case e: Throwable =>
        LoggerFactory.getLogger(this.getClass).error("Failed to close ... will quit directly", e)
    }

    try {
      CommonUtils.retry(CommonConst.driverClosingRetries) {
        CommonUtils.withTimeout(CommonConst.driverClosingTimeout) {

          self.quit()
        }
        Thread.sleep(1000)
      }
    } catch {
      case e: ConnectException =>
        LoggerFactory.getLogger(this.getClass).warn(s"${e.getMessage}, assuming already quit")
    }
  }

  override def silentOnError(ee: Throwable): Boolean = {
    ee match {
      case _: NoSuchSessionException => true
      case _                         => false
    }
  }
}
