package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.doc._
import com.tribbloids.spookystuff.dsl.DocFilters
import com.tribbloids.spookystuff.extractors.Extractor
import com.tribbloids.spookystuff.extractors.impl.Lit
import com.tribbloids.spookystuff.row.{FetchedRow, SpookySchema}
import com.tribbloids.spookystuff.session.Session

import java.util.Date

/**
  * Export a page from the browser or http client
  * the page an be anything including HTML/XML file, image, PDF file or JSON string.
  */
@SerialVersionUID(564570120183654L)
abstract class Export extends Named {

  def filter: DocFilter = DocFilters.Bypass

  final override def outputNames: Set[CSSQuery] = Set(this.name)

  final override def skeleton: Option[Export.this.type] = None //have not impact to driver

  final def doExe(session: Session): Seq[DocOption] = {
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
      case other: DocOption =>
        other
    }
  }

  def doExeNoName(session: Session): Seq[DocOption]
}

trait WaybackLike {

  def wayback: Extractor[Long]
}

trait Wayback extends WaybackLike {

  var wayback: Extractor[Long] = _

  def waybackTo(date: Extractor[Date]): this.type = {
    this.wayback = date.andFn(_.getTime)
    this
  }

  def waybackTo(date: Date): this.type = this.waybackTo(Lit(date))

  def waybackToTimeMillis(time: Extractor[Long]): this.type = {
    this.wayback = time
    this
  }

  def waybackToTimeMillis(date: Long): this.type = this.waybackToTimeMillis(Lit(date))

  //has to be used after copy
  protected def injectWayback(
      wayback: Extractor[Long],
      pageRow: FetchedRow,
      schema: SpookySchema
  ): Option[this.type] = {
    if (wayback == null) Some(this)
    else {
      val valueOpt = wayback.resolve(schema).lift(pageRow)
      valueOpt.map { v =>
        this.wayback = Lit.erased(v)
        this
      }
    }
  }
}
