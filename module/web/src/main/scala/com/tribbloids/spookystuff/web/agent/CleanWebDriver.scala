package com.tribbloids.spookystuff.web.agent

import com.tribbloids.spookystuff.CommonConst
import com.tribbloids.spookystuff.agent.DriverLike
import com.tribbloids.spookystuff.commons.lifespan.Cleanable.Lifespan
import com.tribbloids.spookystuff.commons.{CommonUtils, TreeException}
import org.openqa.selenium.{NoSuchSessionException, WebDriver, WebDriverException}

import scala.language.implicitConversions
import scala.util.Try

object CleanWebDriver {

  implicit def unwrap(v: CleanWebDriver): WebDriver = v.bundle.driver
}

class CleanWebDriver(
    val bundle: WebDriverBundle,
    override val _lifespan: Lifespan = Lifespan.TaskOrJVM().forShipping,
    val afterClean: () => Unit = () => {}
) extends DriverLike {

  def driver = bundle.driver
  def service = bundle.service

  override def cleanImpl(): Unit = {

    CommonUtils.retry(CommonConst.driverClosingRetries, interval = 1000L) {
      CommonUtils.withTimeout(CommonConst.driverClosingTimeout, interrupt = false) {

        val toBeExecuted = Seq(
          Try(bundle.driver.quit()),
          Try(bundle.service.stop())
        )

        try {
          TreeException.&&&(toBeExecuted)
        } catch {
          case e: Throwable =>
            new WebDriverException(
              "Failed to clean driver " + bundle.driver,
              e
            )
        }

      }
    }

    afterClean()
  }

  override def silentOnError(ee: Throwable): Boolean = {
    ee match {
      case _: NoSuchSessionException => true
      case _                         => false
    }
  }
}
