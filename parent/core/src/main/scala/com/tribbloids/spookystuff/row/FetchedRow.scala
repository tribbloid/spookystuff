package com.tribbloids.spookystuff.row

import com.tribbloids.spookystuff.doc.Observation.{Failure, Success}
import com.tribbloids.spookystuff.doc._

object FetchedRow {

  lazy val blank: FetchedRow = FetchedRow()
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

  @transient lazy val dataRowWithScope: DataRow.WithScope = DataRow.WithScope(dataRow, observations.map(_.uid))

  def squash: SquashedRow = {
    SquashedRow
      .ofData(
        dataRowWithScope
      )
      .cache(observations)
  }

  @transient lazy val success: Seq[Success] = observations.collect {
    case v: Success => v
  }

  @transient lazy val failure: Seq[Failure] = observations.collect {
    case v: Failure => v
  }

  @transient lazy val docs: Seq[Doc] = observations.collect {
    case v: Doc => v
  }

  @transient lazy val onlyDoc: Option[Doc] = {

    if (docs.size > 1)
      throw new UnsupportedOperationException(
        "Ambiguous key referring to multiple pages"
      )
    else docs.headOption
  }

  @transient lazy val converted: FetchedRow = {

    this.copy(
      observations = this.observations.map {
        case v: Doc => v.converted
        case v      => v
      }
    )
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
