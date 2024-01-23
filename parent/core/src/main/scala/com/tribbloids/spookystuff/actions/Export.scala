package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.doc._
import com.tribbloids.spookystuff.dsl.DocFilterImpl
import com.tribbloids.spookystuff.session.Session

/**
  * Export a page from the browser or http client the page an be anything including HTML/XML file, image, PDF file or
  * JSON string.
  */
@SerialVersionUID(564570120183654L)
abstract class Export extends Named {

  def filter: DocFilter = DocFilterImpl.Bypass

  final override def outputNames: Set[CSSQuery] = Set(this.name)

  final override def skeleton: Option[Export.this.type] = None // have not impact to driver

  final def doExe(session: Session): Seq[Observation] = {
    val results = doExeNoName(session)
    results.map {
      case doc: Doc =>
        try {
          filter.apply(doc -> session)
        } catch {
          case e: Exception =>
            val message = getSessionExceptionMessage(session, Some(doc))
            val wrapped = DocWithError(doc, message, e)

            throw wrapped
        }
      case other: Observation =>
        other
    }
  }

  def doExeNoName(session: Session): Seq[Observation]
}
