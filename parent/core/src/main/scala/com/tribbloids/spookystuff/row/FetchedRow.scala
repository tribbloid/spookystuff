package com.tribbloids.spookystuff.row

import com.tribbloids.spookystuff.doc._

object FetchedRow {

  lazy val blank: FetchedRow = FetchedRow()

  //  def ofDatum(dataRow: DataRow): SquashedRow = ofData(Seq(dataRow))

  //  lazy val blank: SquashedRow = {
  //    ofData(Seq(DataRow.blank))
  //  }
}

/**
  * abstracted data structure where expression can be resolved. not the main data structure in execution plan,
  * SquashedPageRow is
  */
case class FetchedRow(
    dataRow: DataRow = DataRow.blank,
    // TODO: scope here is only to simplify SquashedRow.extract, all references in it are already lost
    observations: Seq[Observation] = Seq.empty,
    ordinal: Int = 0
) {

  lazy val dataRowWithScope: DataRow.WithScope = DataRow.WithScope(dataRow, observations.map(_.uid))

  def squash: SquashedRow = {
    SquashedRow
      .ofData(
        dataRowWithScope
      )
      .cache(observations)
  }

  lazy val docs: Seq[Doc] = observations.flatMap {
    case page: Doc => Some(page)
    case _         => None
  }

  lazy val onlyDoc: Option[Doc] = {
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
