package com.tribbloids.spookystuff.row

import ai.acyclic.prover.commons.util.PathMagnet
import com.tribbloids.spookystuff.doc.*
import com.tribbloids.spookystuff.doc.Observation.{DocUID, Error, Success}
import com.tribbloids.spookystuff.execution.{ExecutionContext, FlatMapPlan}
import com.tribbloids.spookystuff.row.AgentRow.ObservationsScaffold
import com.tribbloids.spookystuff.row.AgentRow.ObservationsScaffold.SingletonScaffold
import com.tribbloids.spookystuff.row.Data.Scope

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions
import scala.reflect.ClassTag

object AgentRow {

  implicit def asObservations[D](self: AgentRow[D]): self.Observations[Observation] = self.observations

  implicit class _seqView[D](self: Seq[AgentRow[D]]) {

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

  trait ObservationsScaffold[T <: Observation] {
    def batch: Seq[T]
    def agentState: AgentState
  }

  object ObservationsScaffold {

    implicit def _asBatch[T <: Observation](v: ObservationsScaffold[T]): Seq[T] = v.batch

    implicit class _asElements(
        self: ObservationsScaffold[Doc]
    ) extends Elements[Unstructured] {

      override def unbox: Seq[Unstructured] = {

        val seq = self.flatMap { doc =>
          doc.rootOpt
        }
        Elements(seq)
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

    trait SingletonScaffold[T <: Observation] extends ObservationsScaffold[T] {

      def value: T
    }

    implicit def _asObservation[T <: Observation](v: ObservationsScaffold.SingletonScaffold[T]): T = v.value
    implicit def _asContent[T <: Doc](v: ObservationsScaffold.SingletonScaffold[T]): Content = v.value.content
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

  lazy val observations: Observations[Observation] = {

    val seq = effectiveScope.observationUIDs.map { uid =>
      agentState.lookup(uid)
    }

    new Observations(seq)
  }

  type DocsVieq = Observations[Doc]

//  lazy val docs: DocsVieq = observations.docs
//  lazy val succeeded: Observations[Success] = observations.succeeded
//  lazy val failed: Observations[Error] = observations.failed

  class Observations[T <: Observation](override val batch: Seq[T]) extends ObservationsScaffold[T] {

    override def agentState: AgentState = AgentRow.this.agentState

    def collect[R <: Observation](fn: PartialFunction[T, R]): Observations[R] =
      new Observations[R](batch.collect(fn))

    @transient lazy val succeeded: Observations[Success] = collect {
      case v: Success => v
    }

    @transient lazy val failed: Observations[Error] = collect {
      case v: Error => v
    }

    @transient lazy val docs: Observations[Doc] = collect {
      case v: Doc => v
    }

    def byName(name: String): Observations[T] = collect {
      case v if v.name == name => v
    }

    def apply(name: String): Observations[T] = byName(name)

    class Singleton(val value: T) extends Observations[T](Seq(value)) with SingletonScaffold[T] {}

    @transient lazy val headOption: Option[Singleton] = batch.headOption.map { doc =>
      new Singleton(doc)
    }

    def head: Singleton = headOption
      .getOrElse(throw new UnsupportedOperationException("No doc found"))

    @transient lazy val only: Singleton = {

      if (batch.size > 1) throw new UnsupportedOperationException("Ambiguous key referring to multiple docs")
      else head
    }

    @transient lazy val normalised: Observations[Observation] = docs.collect { doc =>
      doc.converted
    }

    @transient lazy val forAuditing: Observations[Doc] = docs.collect {
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
