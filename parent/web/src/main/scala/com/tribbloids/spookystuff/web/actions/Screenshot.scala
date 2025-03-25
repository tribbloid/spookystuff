package com.tribbloids.spookystuff.web.actions

import com.tribbloids.spookystuff.actions.{DocFilter, Export, Wayback}
import com.tribbloids.spookystuff.agent.Agent
import com.tribbloids.spookystuff.doc.*
import com.tribbloids.spookystuff.doc.Observation.DocUID
import com.tribbloids.spookystuff.web.conf.Web
import org.openqa.selenium.{OutputType, TakesScreenshot}

case class Screenshot(
    override val filter: DocFilter = DocFilter.defaultForImage
) extends Export
    with WebAction
    with Wayback {

  override def doExeNoName(agent: Agent): Seq[Doc] = {

    val webDriver = agent.getDriver(Web)

    val raw: Array[Byte] = webDriver.self match {
      case ts: TakesScreenshot => ts.getScreenshotAs(OutputType.BYTES)
      case _                   => throw new UnsupportedOperationException("driver doesn't support screenshot")
    }

    val page = Doc(
      DocUID((agent.backtrace :+ this).toList)(),
      webDriver.getCurrentUrl,
      Some("image/png")
    )().setRaw(raw)
    page

    Seq(page)
  }
}

object Screenshot {

  object QuickScreenshot extends Screenshot(DocFilter.Bypass)
  object ErrorScreenshot extends Screenshot(DocFilter.Bypass)
}
