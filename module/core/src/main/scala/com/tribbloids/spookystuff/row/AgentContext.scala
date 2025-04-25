package com.tribbloids.spookystuff.row

import ai.acyclic.prover.commons.spark.serialization.NOTSerializable
import ai.acyclic.prover.commons.util.PathMagnet
import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions.Trace
import com.tribbloids.spookystuff.doc.Observation.{Error, Success}
import com.tribbloids.spookystuff.doc.{Content, Doc, ManyNodes, Node, Observation}
import com.tribbloids.spookystuff.execution.ExecutionContext
import com.tribbloids.spookystuff.io.WriteMode
import com.tribbloids.spookystuff.row.AgentContext.Trajectory

import scala.collection.MapView
import scala.language.implicitConversions

object AgentContext {

  case class Static(
      localityGroup: LocalityGroup,
      override val ec: ExecutionContext
  ) extends AgentContext {

    // TODO: This class is minimal before imperative/define-by-run Agent API:
    //  Delta can only interact with DataRow, interacting with Agent is not possible
    //  as a result, there is no point of multiple agents using 1 LocalityGroup, nothing needs to be shared

    @transient lazy val trajectoryBase: Seq[Observation] = {

      rollout.withCtx.apply(ec.ctx).trajectory
    }

    def rollout: Trace.Rollout = localityGroup.rollout
  }

  @Deprecated // TODO: finish this
  case class AdHoc(override val ec: ExecutionContext) extends AgentContext {
    override def localityGroup: LocalityGroup = ???
    override def trajectoryBase: Seq[Observation] = ???
  }

  object Trajectory {

    implicit def _asBatch[T <: Observation](v: Trajectory[T]): Seq[T] = v.base

    class _1[+T <: Observation](
        val value: T,
        ctx: SpookyContext,
        @transient agentState: AgentContext
    ) extends Trajectory[T](
          Seq(value),
          ctx,
          agentState: AgentContext
        )

    implicit def _asObservation[T <: Observation](v: Trajectory._1[T]): T = v.value
    implicit def _asContent[T <: Doc](v: Trajectory._1[T]): Content = v.value.content

    def apply[T <: Observation](
        base: Seq[T],
        @transient agentState: AgentContext
    ) = new Trajectory[T](base, agentState.ctx, agentState)
  }

  class Trajectory[+T <: Observation](
      val base: Seq[T],
      val ctx: SpookyContext,
      @transient val agentState: AgentContext
  ) extends ManyNodes[Node] {

    lazy val lookup: MapView[Observation.DocUID, Observation] = {

      lazy val lookup_multi: Map[Observation.DocUID, Seq[Observation]] = {

        base.groupBy { oo =>
          oo.uid
        }
      }

      lookup_multi.view.mapValues { v =>
        require(v.size == 1, "multiple observations with identical UID")
        v.head
      }
    }

    import Trajectory.*
    @transient lazy val succeeded: Trajectory[Success] = collect {
      case v: Success => v
    }
    @transient lazy val failed: Trajectory[Error] = collect {
      case v: Error => v
    }
    @transient lazy val docs: Trajectory[Doc] = collect {
      case v: Doc => v
    }
    @transient lazy val normalised: Trajectory[Observation] = {

      collect {
        case v: Doc => v.normalised
        case others => others
      }
    }
    @transient lazy val docsForAuditing: Trajectory[Doc] = collect {
      Function.unlift { v =>
        v.docForAuditing
      }
    }
    @transient lazy val headOption: Option[_1[T]] = base.headOption.map { doc =>
      new Trajectory._1(doc, ctx, agentState)
    }
    @transient lazy val only: _1[T] = {

      if (base.size > 1) throw new UnsupportedOperationException("Ambiguous key referring to multiple docs")
      else head
    }

    def byName(name: String): Trajectory[T] = collect {
      case v if v.name == name => v
    }

    def collect[R <: Observation](fn: PartialFunction[T, R]): Trajectory[R] =
      Trajectory[R](base.collect(fn), agentState)

    def head: _1[T] = headOption
      .getOrElse(throw new UnsupportedOperationException("No doc found"))

    override def nodeSeq: Seq[Node] = {

      val seq = docs.flatMap { doc =>
        doc.rootOpt
      }
      seq
    }

    def save(
        path: PathMagnet.URIPath,
        extension: Option[String] = None,
        mode: WriteMode = WriteMode.ErrorIfExists
    ): Unit = {

      docs.zipWithIndex.foreach {

        case (doc, _) =>
          doc.prepareSave(ctx, mode).save(path, extension)
      }
    }

  }

}

/**
  * [[com.tribbloids.spookystuff.agent.Agent]]'s interactive API, one of the two APIs [[Delta]] can interact with
  *
  * always contains a [[LocalityGroup]] for efficient [[Delta]] execution
  *
  * always bind to exactly one [[com.tribbloids.spookystuff.agent.Agent]]
  *
  * @param group
  *   shared agent interaction that improves efficiency
  */
trait AgentContext extends NOTSerializable {

  // TODO: This class is minimal before imperative/define-by-run Agent API:
  //  Delta can only interact with DataRow, interacting with Agent is not possible
  //  as a result, there is no point of multiple agents using 1 LocalityGroup, nothing needs to be shared

  @transient lazy val trajectory = Trajectory(trajectoryBase, this)

  def localityGroup: LocalityGroup

  def ec: ExecutionContext

  def trajectoryBase: Seq[Observation]

  final def ctx: SpookyContext = ec.ctx
}
