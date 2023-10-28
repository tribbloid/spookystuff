package com.tribbloids.spookystuff.row

import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.doc._

object FetchedRow {

//  def noData(
//      fetched: Seq[DocOption] = Seq()
//  ): FetchedRow = {
//    FetchedRow(DataRow(), fetched)
//  }
  // TODO: delete, should only initialize from SquashedRow
}

/**
  * abstracted data structure where expression can be resolved. not the main data structure in execution plan,
  * SquashedPageRow is
  */
case class FetchedRow(
    dataRow: DataRow = DataRow(),
    fetched: Seq[DocOption] = Seq()
) extends Serializable {

  // TODO: trace implementation is not accurate: the last backtrace has all previous exports removed
  def squash(): SquashedRow = SquashedRow(
    Array(dataRow),
    TraceView.withDocs(docs = fetched)
  )

  def docs: Seq[Doc] = fetched.flatMap {
    case page: Doc => Some(page)
    case _         => None
  }

//  def noDocs: Seq[NoDoc] = fetched.flatMap {
//    case noPage: NoDoc => Some(noPage)
//    case _             => None
//  }

  def getOnlyDoc: Option[Doc] = {
    val pages = this.docs

    if (pages.size > 1) throw new UnsupportedOperationException("Ambiguous key referring to multiple pages")
    else pages.headOption
  }

  def getDoc(keyStr: String): Option[Doc] = {

    //    if (keyStr == Const.onlyPageWildcard) return getOnlyPage

    val pages = this.docs.filter(_.name == keyStr)

    if (pages.size > 1) throw new UnsupportedOperationException("Ambiguous key referring to multiple pages")
    else pages.headOption
  }

  // TODO: remove, resolved at schema level
//  def getUnstructured(alias: Alias): Option[Unstructured] = {
//
//    val page = getDoc(alias.name)
//    val value = dataRow.getTyped[Unstructured](alias)
//
//    if (page.nonEmpty && value.nonEmpty)
//      throw new UnsupportedOperationException("Ambiguous key referring to both pages and data")
//    else page.map(_.root).orElse(value)
//  }
}
