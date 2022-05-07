package com.tribbloids.spookystuff.web.actions

import com.tribbloids.spookystuff.Const
import com.tribbloids.spookystuff.actions.{Export, Wayback}
import com.tribbloids.spookystuff.doc._
import com.tribbloids.spookystuff.dsl.DocFilterImpl
import com.tribbloids.spookystuff.row.{FetchedRow, SpookySchema}
import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.web.conf.Web
import org.openqa.selenium.{OutputType, TakesScreenshot}

case class Screenshot(
    override val filter: DocFilter = Const.defaultImageFilter
) extends Export
    with WebAction
    with Wayback {

  override def doExeNoName(session: Session): Seq[Doc] = {

    val pageOpt = session.Drivers.get(Web).map { webDriver =>
      val content = webDriver.self match {
        case ts: TakesScreenshot => ts.getScreenshotAs(OutputType.BYTES)
        case _                   => throw new UnsupportedOperationException("driver doesn't support screenshot")
      }

      val page = new Doc(
        DocUID((session.backtrace :+ this).toList, this)(),
        webDriver.getCurrentUrl,
        content,
        Some("image/png")
      )
      page
    }

    pageOpt.map(v => Seq(v)).getOrElse(Nil)
  }

  override def doInterpolate(pageRow: FetchedRow, schema: SpookySchema): Option[Screenshot.this.type] = {
    this.copy().asInstanceOf[this.type].injectWayback(this.wayback, pageRow, schema)
  }
}

object Screenshot {

  object QuickScreenshot extends Screenshot(DocFilterImpl.Bypass)
  object ErrorScreenshot extends Screenshot(DocFilterImpl.Bypass)
}
