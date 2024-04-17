package com.tribbloids.spookystuff.row

import ai.acyclic.prover.commons.util.Magnet.OptionMagnet
import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.commons.serialization.NOTSerializable
import com.tribbloids.spookystuff.doc.*
import com.tribbloids.spookystuff.doc.Observation.{DocUID, Failure, Success}
import com.tribbloids.spookystuff.execution.FlatMapPlan
import com.tribbloids.spookystuff.row.Data.WithScope

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
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

  // TODO: question: should these be part of FetchedDataset?
  //  - do we need a tracer interface (like in dual-number autograd in jax) to enable define-by-run?
  //  - if so, the paradigm for FetchedRow[T] & FetchedRow[NamedTuple] could be different!
  //  - in define-by-run, the tracer is just a thin wrapper of a function argument,
  //  it only records operation performed on it if the function explicitly ask for it, otherwise it degrades to its value (can still trace if it is used tho)
  //  - can we afford this luxury? how many functions can be defined to ask for it? can we weaken the arg types of functions without extra work?

  case class SeqView[D](self: Seq[FetchedRow[D]]) {

    // the following functions are also available for a single FetchedRow, treated as Seq(v)
//    def dummy(): Any = ???

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
    ): FlatMapPlan.Batch[O] = {

      self.flatMap { row =>
        fn(row).map { dd =>
          row.dataWithScope.copy(
            data = dd
          )
        }
      }
    }

    // TODO: move to sql module
//    def withColumns[
//        IT <: Tuple,
//        O,
//        OT <: Tuple
//    ](
//        fn: FetchedRow[D] :=> Seq[O]
//    )(
//        implicit
//        toRow1: RowInternal.ofData.Lemma[D, Row[IT]],
//        toRow2: RowInternal.ofData.Lemma[O, Row[OT]],
//        merge: ElementWisePoly.preferRight.LemmaAtRows[IT, OT]
//    ): ChainPlan.Out[merge.Out] = {
//
//      val _fn = fn.andThen { outs =>
//        outs.map { out =>
//          val row1 = toRow1(self.head.data)
//          val row2 = toRow2(out)
//          merge(row1 -> row2)
//        }
//      }
//
//      select(_fn)
//    }
//
//    def explode[O](
//        fn: FetchedRow[D] => Seq[O]
//    ): ChainPlan.Out[O] = {
//
//      ???
//    }
//
  }

  implicit def toSeqView[D](self: Seq[FetchedRow[D]]): SeqView[D] = SeqView(self)
}

/**
  * abstracted data structure where expression can be resolved. not the main data structure in execution plan, Not
  * serializable, has to be created from [[SquashedRow]] on the fly
  */
case class FetchedRow[D](
    data: D,
    observations: Seq[Observation] = Seq.empty,
    ordinalIndex: Int = 0,
    private val ctx: OptionMagnet[SpookyContext] = None
) extends NOTSerializable {

  @transient lazy val dataWithScope: Data.WithScope[D] = Data.WithScope(data, observations.map(_.uid), ordinalIndex)

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

  object rescope {

    // make sure no pages with identical name can appear in the same group.
    lazy val byDistinctNames: Seq[WithScope[D]] = {
      val outerBuffer: ArrayBuffer[Seq[DocUID]] = ArrayBuffer()

      object innerBuffer {
        val refs: mutable.ArrayBuffer[DocUID] = ArrayBuffer()
        val names: mutable.HashSet[String] = mutable.HashSet[String]()

        def add(uid: DocUID): Unit = {
          refs += uid
          names += uid.name
        }

        def clear(): Unit = {
          refs.clear()
          names.clear()
        }
      }

      dataWithScope.scopeUIDs.foreach { uid =>
        if (innerBuffer.names.contains(uid.name)) {
          outerBuffer += innerBuffer.refs.toList
          innerBuffer.clear()
        }
        innerBuffer.add(uid)
      }
      outerBuffer += innerBuffer.refs.toList // always left, have at least 1 member

      outerBuffer.zipWithIndex.map {
        case (v, i) =>
          dataWithScope.copy(scopeUIDs = v, ordinal = i)
      }.toSeq
    }
  }

  def docs: DocSelection = {
    new DocSelection(observations.collect {
      case v: Doc => v
    })
  }

  object DocSelection {

    implicit def asDoc(v: DocSelection.Only): Doc = v.value

    implicit def asBatch(v: DocSelection): Seq[Doc] = v.values

    class Only(val value: Doc) extends DocSelection(Seq(value)) {}
  }

  class DocSelection(val values: Seq[Doc]) {

    def get(name: String): DocSelection = {

      lazy val pages: Seq[Doc] = values.filter(_.name == name)
      new DocSelection(pages)
    }

    def apply(name: String): DocSelection = get(name)

    lazy val headOption: Option[DocSelection.Only] = values.headOption.map { doc =>
      new DocSelection.Only(doc)
    }

    def head: DocSelection.Only = headOption.getOrElse(throw new UnsupportedOperationException("No doc found"))

    lazy val only: DocSelection.Only = {

      if (values.size > 1) throw new UnsupportedOperationException("Ambiguous key referring to multiple docs")
      else head
    }

    def saveContent(
        path: String,
        extension: Option[String], // set to
        overwrite: Boolean = false
    ): Unit = {

      values.zipWithIndex.foreach {

        case (doc, _) =>
          val saveParts = Seq(path) ++ extension

          doc.save(ctx.get, overwrite)(saveParts)
      }
    }
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
