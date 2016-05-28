package com.tribbloids.spookystuff.row

import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.doc._

object FetchedRow {

  def apply(
             dataRow: DataRow = DataRow(),
             pageLikes: Seq[Fetched] = Seq()
           ): FetchedRow = dataRow -> pageLikes

}

/**
  * abstracted data structure where expression can be resolved.
  * not the main data structure in execution plan, SquashedPageRow is
  */
case class FetchedRowView(
                           self: FetchedRow
                         ) {

  def dataRow: DataRow = self._1
  def pageLikes: Seq[Fetched] = self._2

  //TODO: trace implementation is not accurate: the last backtrace has all previous exports removed
  def squash = SquashedFetchedRow(
    Array(dataRow),
    _fetched = pageLikes.toArray
  )

  def pages: Seq[Doc] = pageLikes.flatMap {
    case page: Doc => Some(page)
    case _ => None
  }

  def noPages: Seq[NoDoc] = pageLikes.flatMap {
    case noPage: NoDoc => Some(noPage)
    case _ => None
  }

  def getOnlyPage: Option[Doc] = {
    val pages = this.pages

    if (pages.size > 1) throw new UnsupportedOperationException("Ambiguous key referring to multiple pages")
    else pages.headOption
  }

  def getPage(keyStr: String): Option[Doc] = {

    //    if (keyStr == Const.onlyPageWildcard) return getOnlyPage

    val pages = this.pages.filter(_.name == keyStr)

    if (pages.size > 1) throw new UnsupportedOperationException("Ambiguous key referring to multiple pages")
    else pages.headOption
  }

  def getUnstructured(field: Field): Option[Unstructured] = {

    val page = getPage(field.name)
    val value = dataRow.getTyped[Unstructured](field)

    if (page.nonEmpty && value.nonEmpty) throw new UnsupportedOperationException("Ambiguous key referring to both pages and data")
    else page.orElse(value)
  }

  @transient lazy val dryrun: DryRun = pageLikes.toList.map(_.uid.backtrace).distinct

}