package com.tribbloids.spookystuff.row

import com.tribbloids.spookystuff.doc.Observation.{Failure, Success}
import com.tribbloids.spookystuff.doc._
import com.tribbloids.spookystuff.execution.ChainPlan
import com.tribbloids.spookystuff.frameless.TypedRowInternal.ElementWiseMethods
import com.tribbloids.spookystuff.frameless.{Tuple, TypedRow, TypedRowInternal}

import scala.language.implicitConversions

object FetchedRow {

  lazy val blank: FetchedRow[Unit] = FetchedRow(())

  /**
    * providing the following APIs:
    *
    * (functional)
    *   - flatMapWithScope(FetchedRow[D] => Seq[ WithScope[D] ])
    *   - flatMap/select(D => Seq[O]) (heavy use of >< to compose fields)
    *     - TypedRowFunctions.explode() can be used on a field
    *   - map(D => O)
    *   - explodeScope
    *
    * (schematic, requires TypedRow, can coerce D into one if necessary)
    *   - withColumns/extract = select(old >< result), cast to TypedRow if necessary
    *
    * wide operations (fetch/fork/explore) will not be part of this API
    *
    * TODO: Question: will this be carried smoothly to fork (select from [[Data.Forking]]) & explore (select from
    * [[Data.Exploring]])? I believe so, and it should be easy
    */

  case class SeqView[D](self: Seq[FetchedRow[D]]) {

    // the following functions are also available for a single FetchedRow, treated as Seq(v)
    def dummy(): Any = ???

    def withNormalisedDocs: Seq[FetchedRow[D]] = {

      self.map(v =>
        v.copy(
          observations = v.observations.map {
            case v: Doc => v.normalised
            case v      => v
          }
        )
      )
    }

    def select[O](
        fn: FetchedRow[D] => Seq[O]
    ): ChainPlan.Out[O] = {

      self.flatMap { row =>
        fn(row).map { dd =>
          row.dataWithScope.copy(
            data = dd
          )
        }
      }
    }

    def withColumns[
        IT <: Tuple,
        O,
        OT <: Tuple
    ](
        fn: FetchedRow[D] => Seq[O]
    )(
        implicit
        toRow1: TypedRowInternal.ofData.Target[D, TypedRow[IT]],
        toRow2: TypedRowInternal.ofData.Target[O, TypedRow[OT]],
        merge: ElementWiseMethods.preferRight.Lemma[IT, OT]
    ): ChainPlan.Out[merge.Out] = {

      val _fn = fn.andThen { outs =>
        outs.map { out =>
          val row1 = toRow1(self.head.data)
          val row2 = toRow2(out)
          merge(row1, row2)
        }
      }

      select(_fn)
    }

    def explode[O](
        fn: FetchedRow[D] => Seq[O]
    ): ChainPlan.Out[O] = {

      ???
    }

  }

  implicit def toSeqView[D](self: Seq[FetchedRow[D]]): SeqView[D] = SeqView(self)

  private def __sanity() {

    val seq = Seq(FetchedRow.blank, FetchedRow.blank)

    seq.dummy() // so it is possible
  }
}

/**
  * abstracted data structure where expression can be resolved. not the main data structure in execution plan,
  * SquashedPageRow is
  */
case class FetchedRow[D](
    data: D,
    // TODO: scope here is only to simplify SquashedRow.extract, all references in it are already lost
    observations: Seq[Observation] = Seq.empty,
    ordinal: Int = 0
) {

  @transient lazy val dataWithScope: Data.WithScope[D] = Data.WithScope(data, observations.map(_.uid), ordinal)

  def squash: SquashedRow[D] = {
    SquashedRow
      .ofData(
        dataWithScope
      )
      .cache(observations)
  }

  @transient lazy val succeeded: Seq[Success] = observations.collect {
    case v: Success => v
  }

  @transient lazy val failed: Seq[Failure] = observations.collect {
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

  def getDoc(keyStr: String): Option[Doc] = {

    //    if (keyStr == Const.onlyPageWildcard) return getOnlyPage

    val pages = this.docs.filter(_.name == keyStr)

    if (pages.size > 1) throw new UnsupportedOperationException("Ambiguous key referring to multiple pages")
    else pages.headOption
  }

//  def getUnstructured(field: Field): Option[Unstructured] = {
//
//    val page = getDoc(field.name)
//    val value = data.getTyped[Unstructured](field)
//
//    if (page.nonEmpty && value.nonEmpty)
//      throw new UnsupportedOperationException("Ambiguous key referring to both pages and data")
//    else page.map(_.root).orElse(value)
//  }
}
