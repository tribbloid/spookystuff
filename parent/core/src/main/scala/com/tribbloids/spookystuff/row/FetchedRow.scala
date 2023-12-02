package com.tribbloids.spookystuff.row

import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.doc._

object FetchedRow {

  lazy val empty: FetchedRow = FetchedRow()
}

/**
  * abstracted data structure where expression can be resolved. not the main data structure in execution plan,
  * SquashedPageRow is
  */
case class FetchedRow(
    dataRow: DataRow = DataRow(),
    trajectory: Trajectory = Trajectory.empty
) {

  def asBottleneckRow(): BottleneckRow = BottleneckRow(
    Vector(dataRow),
    Trace().setCache(trajectory)
  )

  def docs: Seq[Doc] = trajectory.flatMap {
    case page: Doc => Some(page)
    case _         => None
  }

  def getOnlyDoc: Option[Doc] = {
    val pages = this.docs

    if (pages.size > 1)
      throw new UnsupportedOperationException(
        "Ambiguous key referring to multiple pages"
      )
    else pages.headOption
  }

  def getDoc(keyStr: String): Option[Doc] = {

    //    if (keyStr == Const.onlyPageWildcard) return getOnlyPage

    val pages = this.docs.filter(_.name == keyStr)

    if (pages.size > 1) throw new UnsupportedOperationException("Ambiguous key referring to multiple pages")
    else pages.headOption
  }

  def getUnstructured(field: Field): Option[Unstructured] = {

    val page = getDoc(field.name)
    val value = dataRow.getTyped[Unstructured](field)

    if (page.nonEmpty && value.nonEmpty)
      throw new UnsupportedOperationException("Ambiguous key referring to both pages and data")
    else page.map(_.root).orElse(value)
  }
}
