package com.tribbloids.spookystuff.web.agent

import com.tribbloids.spookystuff.CommonConst
import com.tribbloids.spookystuff.agent.DriverLike
import com.tribbloids.spookystuff.commons.lifespan.Cleanable.Lifespan
import com.tribbloids.spookystuff.commons.{CommonUtils, TreeThrowable}
import org.openqa.selenium.remote.service.DriverService
import org.openqa.selenium.{NoSuchSessionException, WebDriver}

import scala.language.implicitConversions
import scala.util.Try

object CleanWebDriver {

  implicit def unwrap(v: CleanWebDriver): WebDriver = v.self
}

class CleanWebDriver(
    val self: WebDriver,
    val serviceOpt: Option[DriverService] = None,
    override val _lifespan: Lifespan = Lifespan.TaskOrJVM().forShipping,
    val afterClean: () => Unit = () => {}
) extends DriverLike {

  override def cleanImpl(): Unit = {

    CommonUtils.retry(CommonConst.driverClosingRetries, interval = 1000L) {
      CommonUtils.withTimeout(CommonConst.driverClosingTimeout, interrupt = false) {

        val toBeExecuted = Seq(Try(self.quit())) ++ serviceOpt.map(v => Try(v.stop()))

        TreeThrowable.&&&(toBeExecuted)
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
