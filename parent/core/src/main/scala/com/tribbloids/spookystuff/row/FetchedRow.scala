package com.tribbloids.spookystuff.row

import ai.acyclic.prover.commons.spark.serialization.NOTSerializable
import com.tribbloids.spookystuff.doc.*
import com.tribbloids.spookystuff.doc.Observation.{DocUID, Failure, Success}
import com.tribbloids.spookystuff.execution.ChainPlan
import com.tribbloids.spookystuff.row.Data.Scope

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions
import scala.reflect.ClassTag

object FetchedRow {

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

  implicit class SeqView[D](self: Seq[FetchedRow[D]]) {

    // the following functions are also available for a single FetchedRow, treated as Seq(v)
//    def dummy(): Any = ???

//    def withNormalisedDocs: Seq[FetchedRow[D]] = { TODO: gone, normalised doc now selected from row
//
//      self.map(v =>
//        v.copy(
//          observations = v.observations.map {
//            case v: Doc => v.normalised
//            case v      => v
//          }
//        )
//      )
//    }

    // TODO: fold into a SpookyDataset backed by a local/non-distributed data structure
    object flatMap {

      def apply[O: ClassTag](
          fn: ChainPlan.FlatMap._Fn[D, O]
      ): Seq[FetchedRow[O]] = {

        self.flatMap { v =>
          val newData = ChainPlan.FlatMap.normalise(fn).apply(v)

          newData.map { datum =>
            FetchedRow(v.agentState, datum)
          }
        }
      }
    }
    def selectMany: flatMap.type = flatMap

    object map {

      def apply[O: ClassTag](fn: ChainPlan.Map._Fn[D, O]): Seq[FetchedRow[O]] = {

        self.flatMap { v =>
          val newData = ChainPlan.Map.normalise(fn).apply(v)

          newData.map { datum =>
            FetchedRow(v.agentState, datum)
          }

        }
      }
    }
    def select: map.type = map

//    def select[O](
//        fn: FetchedRow[D] => Seq[O]
//    ): ChainPlan.Batch[O] = {
//
//      self.flatMap { row =>
//        fn(row)
//      }
//    }

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

//  implicit def toSeqView[D](self: Seq[FetchedRow[D]]): SeqView[D] = SeqView(self)

//  lazy val blank: Proto[Unit] = Proto((), Nil)
//
//  lazy val blank: FetchedRow[Unit](AgentState)
}

/**
  * abstracted data structure where expression can be resolved. not the main data structure in execution plan, Not
  * serializable, has to be created from [[SquashedRow]] on the fly
  */
case class FetchedRow[D](
    agentState: AgentState,
    data: D
) extends NOTSerializable {

  lazy val effectiveScope: Scope = {

    val result = data match {
      case v: Data.Scoped[_] => v.scope
      case _ =>
        val uids = agentState.trajectory.map(_.uid)
        Scope(uids)
    }

    result
  }

  object rescope {

    // make sure no pages with identical name can appear in the same group.
    lazy val byDistinctNames: Seq[Data.Scoped[D]] = {
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

      effectiveScope.observationUIDs.foreach { uid =>
        if (innerBuffer.names.contains(uid.name)) {
          outerBuffer += innerBuffer.refs.toList
          innerBuffer.clear()
        }
        innerBuffer.add(uid)
      }
      outerBuffer += innerBuffer.refs.toList // always left, have at least 1 member

      outerBuffer.zipWithIndex.map {
        case (v, i) =>
          Data.Scoped(data = data, scope = Scope(v, i))
      }.toSeq
    }
  }

//  def docs: ObservationsView = {
//    new ObservationsView(observations.collect {
//      case v: Doc => v
//    })
//  }

  lazy val observations: ObservationsView[Observation] = {

    val seq = effectiveScope.observationUIDs.map { uid =>
      agentState.lookup(uid)
    }

    new ObservationsView(seq)
  }

  lazy val docs: ObservationsView[Doc] = observations.docs
  lazy val succeeded: ObservationsView[Success] = observations.succeeded
  lazy val failed: ObservationsView[Failure] = observations.failed

  object ObservationsView {

    implicit def asBatch[T <: Observation](v: ObservationsView[T]): Seq[T] = v.self

    class Single[T <: Observation](val value: T) extends ObservationsView(Seq(value)) {}

    implicit def asDoc[T <: Observation](v: ObservationsView.Single[T]): T = v.value

    implicit class DocExtensions(
        self: ObservationsView[Doc]
    ) extends Elements[Unstructured] {

      override def unbox: Seq[Unstructured] = {

        val seq = self.docs.flatMap { doc =>
          doc.rootOpt
        }
        Elements(seq)
      }

      val normalised: Seq[Observation] = self.collect { doc =>
        doc.normalised
      }

      def save(
          path: String,
          extension: Option[String] = None,
          overwrite: Boolean = false
      ): Unit = {

        self.zipWithIndex.foreach {

          case (doc, _) =>
            val saveParts = Seq(path) ++ extension

            doc.save(agentState.ctx, overwrite).as(saveParts)
        }
      }
    }

  }

  class ObservationsView[T <: Observation](val self: Seq[T]) extends NOTSerializable {

    def collect[R <: Observation](fn: PartialFunction[T, R]): ObservationsView[R] =
      new ObservationsView[R](self.collect(fn))

    @transient lazy val succeeded: ObservationsView[Success] = collect {
      case v: Success => v
    }

    @transient lazy val failed: ObservationsView[Failure] = collect {
      case v: Failure => v
    }

    @transient lazy val docs: ObservationsView[Doc] = collect {
      case v: Doc => v
    }

    def ofName(name: String): ObservationsView[T] = collect {
      case v if v.name == name => v
    }

    def apply(name: String): ObservationsView[T] = ofName(name)

    lazy val headOption: Option[ObservationsView.Single[T]] = self.headOption.map { doc =>
      new ObservationsView.Single(doc)
    }

    def head: ObservationsView.Single[T] = headOption
      .getOrElse(throw new UnsupportedOperationException("No doc found"))

    lazy val only: ObservationsView.Single[T] = {

      if (self.size > 1) throw new UnsupportedOperationException("Ambiguous key referring to multiple docs")
      else head
    }

    def forAuditing: ObservationsView[Doc] = collect {
      Function.unlift { v =>
        v.docForAuditing
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
