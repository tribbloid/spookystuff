package com.tribbloids.spookystuff.web.actions

import com.tribbloids.spookystuff.actions.{Export, Wayback}
import com.tribbloids.spookystuff.agent.Harness
import com.tribbloids.spookystuff.doc.*
import com.tribbloids.spookystuff.doc.Observation.ReplayUID
import com.tribbloids.spookystuff.web.agent.CleanWebDriver
import com.tribbloids.spookystuff.web.conf.Web
import org.openqa.selenium.{OutputType, TakesScreenshot}

case class Screenshot() extends Export with WebAction with Wayback {

  override def doExe(agent: Harness): Seq[Doc] = {

    val webDriver: CleanWebDriver = agent.getDriver(Web)

    val raw: Array[Byte] = webDriver.bundle.driver match {
      case ts: TakesScreenshot => ts.getScreenshotAs(OutputType.BYTES)
      case _                   => throw new UnsupportedOperationException("driver doesn't support screenshot")
    }

    val page = Doc(
      ReplayUID((agent.backtrace :+ this).toList)(),
      webDriver.getCurrentUrl,
      Some("image/png")
    )().setRaw(raw)

    Seq(page)
  }
}

object Screenshot {

  object QuickScreenshot extends Screenshot()
  object ErrorScreenshot extends Screenshot()
}
