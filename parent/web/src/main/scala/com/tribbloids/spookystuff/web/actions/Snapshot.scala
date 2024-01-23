package com.tribbloids.spookystuff.web.actions

import com.tribbloids.spookystuff.Const
import com.tribbloids.spookystuff.actions.{Export, Wayback}
import com.tribbloids.spookystuff.doc.Observation.DocUID
import com.tribbloids.spookystuff.doc._
import com.tribbloids.spookystuff.dsl.DocFilterImpl
import com.tribbloids.spookystuff.row.{FetchedRow, SpookySchema}
import com.tribbloids.spookystuff.session.Session
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
  override def doExeNoName(session: Session): Seq[Doc] = {

    val pageOpt = session.Drivers.get(Web).map { webDriver =>
      new Doc(
        DocUID((session.backtrace :+ this).toList, this)(),
        webDriver.getCurrentUrl,
        webDriver.getPageSource.getBytes("UTF8"),
        Some("text/html; charset=UTF-8")
        //      serializableCookies
      )
    }
    //    if (contentType != null) Seq(page.copy(declaredContentType = Some(contentType)))
    pageOpt.map(v => Seq(v)).getOrElse(Nil)
  }

  override def doInterpolate(pageRow: FetchedRow, schema: SpookySchema): Option[Snapshot.this.type] = {
    this.copy().asInstanceOf[this.type].injectWayback(this.wayback, pageRow, schema)
  }
}

object Snapshot {

  // this is used to save GC when invoked by anothor component
  object QuickSnapshot extends Snapshot(DocFilterImpl.Bypass)
  object ErrorDump extends Snapshot(DocFilterImpl.Bypass)
  //  with MessageAPI

}
