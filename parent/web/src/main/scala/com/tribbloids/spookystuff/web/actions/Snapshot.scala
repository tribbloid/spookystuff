package com.tribbloids.spookystuff.web.actions

import com.tribbloids.spookystuff.Const
import com.tribbloids.spookystuff.actions.{DocFilter, Export, Wayback}
import com.tribbloids.spookystuff.doc.Observation.DocUID
import com.tribbloids.spookystuff.doc.*
import com.tribbloids.spookystuff.dsl.DocFilterImpl
import com.tribbloids.spookystuff.agent.Agent
import com.tribbloids.spookystuff.web.conf.Web

/**
  * Export the current page from the browser interact with the browser to load the target page first only for html page,
  * please use wget for images and pdf files always export as UTF8 charset
  */
case class Snapshot(
    override val filter: DocFilter = Const.defaultDocumentFilter
) extends Export
    with WebAction
    with Wayback {

  // all other fields are empty
  override def doExeNoName(agent: Agent): Seq[Doc] = {
    // no effect if WebDriver is missing

    val webDriver = agent.driverOf(Web)

    val doc = Doc(
      DocUID((agent.backtrace :+ this).toList)(),
      webDriver.getCurrentUrl,
      Some("text/html; charset=UTF-8")
      //      serializableCookies
    )().setRaw(webDriver.getPageSource.getBytes("UTF8"))

    //    if (contentType != null) Seq(page.copy(declaredContentType = Some(contentType)))

    Seq(doc)
  }
}

object Snapshot {

  // this is used to save GC when invoked by anothor component
  object QuickSnapshot extends Snapshot(DocFilterImpl.Bypass)
  object ErrorDump extends Snapshot(DocFilterImpl.Bypass)
  //  with MessageAPI

}
