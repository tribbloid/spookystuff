package com.tribbloids.spookystuff.row

import ai.acyclic.prover.commons.util.PathMagnet
import com.tribbloids.spookystuff.doc.*
import com.tribbloids.spookystuff.doc.Observation.{DocUID, Failure, Success}
import com.tribbloids.spookystuff.execution.{ExecutionContext, FlatMapPlan}
import com.tribbloids.spookystuff.row.AgentRow.ObservationSeqViewScaffold
import com.tribbloids.spookystuff.row.AgentRow.ObservationSeqViewScaffold.SingletonMixin
import com.tribbloids.spookystuff.row.Data.Scope

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions
import scala.reflect.ClassTag

object AgentRow {

  /**
    * providing the following APIs:
    *
    * (functional)
    *   - flatMapWithScope(AgentRow[D] => Seq[ WithScope[D] ])
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
  //  - if so, the paradigm for AgentRow[T] & AgentRow[NamedTuple] could be different!
  //  - in define-by-run, the tracer is just a thin wrapper of a function argument,
  //  it only records operation performed on it if the function explicitly ask for it, otherwise it degrades to its value (can still trace if it is used tho)
  //  - can we afford this luxury? how many functions can be defined to ask for it? can we weaken the arg types of functions without extra work?

  implicit class SeqView[D](self: Seq[AgentRow[D]]) {

    // TODO: fold into a SpookyDataset backed by a local/non-distributed data structure
    object flatMap {

      def apply[O: ClassTag](
          fn: FlatMapPlan.FlatMap._Fn[D, O]
      ): Seq[AgentRow[O]] = {

        self.flatMap { v =>
          val newData = FlatMapPlan.FlatMap.normalise(fn).apply(v)

          newData.map { datum =>
            v.copy(data = datum)
          }
        }
      }
    }
    def selectMany: flatMap.type = flatMap

    object map {

      def apply[O: ClassTag](fn: FlatMapPlan.Map._Fn[D, O]): Seq[AgentRow[O]] = {

        self.flatMap { v =>
          val newData = FlatMapPlan.Map.normalise(fn).apply(v)

          newData.map { datum =>
            v.copy(data = datum)
          }

        }
      }
    }
    def select: map.type = map

  }

  trait ObservationSeqViewScaffold[T <: Observation] {
    def batch: Seq[T]
    def agentState: AgentState
  }

  object ObservationSeqViewScaffold {

    implicit def asBatch[T <: Observation](v: ObservationSeqViewScaffold[T]): Seq[T] = v.batch

    trait SingletonMixin[T <: Observation] extends ObservationSeqViewScaffold[T] {

      def value: T
    }

    implicit def asDoc[T <: Observation](v: ObservationSeqViewScaffold.SingletonMixin[T]): T = v.value
  }

  implicit class DocSeqView(
      self: ObservationSeqViewScaffold[Doc]
  ) extends Elements[Unstructured] {

    override def unbox: Seq[Unstructured] = {

      val seq = self.batch.flatMap { doc =>
        doc.rootOpt
      }
      Elements(seq)
    }

    val normalised: Seq[Observation] = self.collect { doc =>
      doc.normalised
    }

    def save(
        path: PathMagnet.URIPath,
        extension: Option[String] = None,
        overwrite: Boolean = false
    ): Unit = {

      self.zipWithIndex.foreach {

        case (doc, _) =>
          doc.prepareSave(self.agentState.ctx, overwrite).save(path, extension)
      }
    }
  }
}

/**
  * abstracted data structure where expression can be resolved. not the main data structure in execution plan, Not
  * serializable, has to be created from [[SquashedRow]] on the fly
  */
case class AgentRow[D](
    localityGroup: LocalityGroup,
    data: D, // deliberately singular
    index: Int,
    ec: ExecutionContext
) {

  {
    agentState.trajectory // by definition, always pre-fetched to serve multiple data in a squashed row
  }

  @transient lazy val agentState: AgentState = {
    AgentState.Impl(localityGroup, ec)
    // will be discarded & recreated when being moved to another computer
    //  BUT NOT the already computed rollout trajectory!, they are carried within the localityGroup
  }

  @transient lazy val squash: SquashedRow[D] = SquashedRow(localityGroup, Seq(data -> index))

  @transient lazy val effectiveScope: Scope = {

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
          Data.Scoped(raw = data, scope = Scope(v, i))
      }.toSeq
    }
  }

  lazy val observations: ObservationSeqView[Observation] = {

    val seq = effectiveScope.observationUIDs.map { uid =>
      agentState.lookup(uid)
    }

    new ObservationSeqView(seq)
  }

  lazy val docs: ObservationSeqView[Doc] = observations.docs
  lazy val succeeded: ObservationSeqView[Success] = observations.succeeded
  lazy val failed: ObservationSeqView[Failure] = observations.failed

  class ObservationSeqView[T <: Observation](override val batch: Seq[T]) extends ObservationSeqViewScaffold[T] {

    override def agentState: AgentState = AgentRow.this.agentState

    def collect[R <: Observation](fn: PartialFunction[T, R]): ObservationSeqView[R] =
      new ObservationSeqView[R](batch.collect(fn))

    @transient lazy val succeeded: ObservationSeqView[Success] = collect {
      case v: Success => v
    }

    @transient lazy val failed: ObservationSeqView[Failure] = collect {
      case v: Failure => v
    }

    @transient lazy val docs: ObservationSeqView[Doc] = collect {
      case v: Doc => v
    }

    def ofName(name: String): ObservationSeqView[T] = collect {
      case v if v.name == name => v
    }

    def apply(name: String): ObservationSeqView[T] = ofName(name)

    type Singleton = SingletonMixin[T]

    @transient lazy val headOption: Option[Singleton] = batch.headOption.map { doc =>
      new ObservationSeqView[T](Seq(doc)) with SingletonMixin[T] {
        override def value: T = doc
      }
    }

    def head: SingletonMixin[T] = headOption
      .getOrElse(throw new UnsupportedOperationException("No doc found"))

    @transient lazy val only: SingletonMixin[T] = {

      if (batch.size > 1) throw new UnsupportedOperationException("Ambiguous key referring to multiple docs")
      else head
    }

    @transient lazy val forAuditing: ObservationSeqView[Doc] = collect {
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
