package com.tribbloids.spookystuff.row

import ai.acyclic.prover.commons.function.Impl
import ai.acyclic.prover.commons.function.Impl.Fn
import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions.{Trace, TraceSet}
import com.tribbloids.spookystuff.commons.serialization.NOTSerializable
import com.tribbloids.spookystuff.doc.Observation
import com.tribbloids.spookystuff.execution.ChainPlan

import scala.language.implicitConversions

object SquashedRow {

  def ofData[D](dataWithScope: Data.WithScope[D]*): SquashedRow[D] = {

    SquashedRow(
      AgentState.ofTrace(Trace.NoOp),
      dataWithScope
    )
  }

//  def ofDatum(dataRow: DataRow): SquashedRow = ofData(Seq(dataRow))

//  lazy val blank: SquashedRow = {
//    ofData(Seq(DataRow.blank))
//  }
  // TODO: gone, use FetchedRow.toSquashedRow()

  case class WithSchema[D](
      self: SquashedRow[D],
      schema: SpookySchema
  ) {

    @transient lazy val withCtx: self._WithCtx = self.withCtx(schema.ctx)

    /**
      * operation is applied per unSquashed row, each row may yield multiple runnable traces
      *
      * this function will never remove a single unSquashed row, in worst case, each row yield an empty trace
      *
      * @return
      *   a mapping from runnable trace to data
      */
//    def interpolateAndRewrite(
//        traces: TraceSet
//    ): Seq[(Trace, DataView)] = {
//
//      val pairs: Seq[(DataView, TraceSet.NonEmpty)] = withCtx.unSquash.map { row =>
//        val raw = traces.flatMap { trace =>
//          val rewritten = trace.interpolateAndRewrite(row, schema)
//          rewritten
//        }
//
//        row.dataRow -> TraceSet(raw).avoidEmpty
//      }
//
//      val result = pairs.flatMap {
//        case (d, ts) =>
//          ts.map(t => t -> d)
//      }
//
//      result
//    }
  }

  object WithSchema {

    implicit def unbox[D](v: WithSchema[D]): v.self._WithCtx = v.withCtx
  }
}

case class SquashedRow[D](
    agentState: AgentState,
    dataSeq: Seq[Data.WithScope[D]]
) extends SpookyContext.CanRunWith {
  // can only support 1 agent
  // will trigger a fork if not all agent actions were captured by the LocalityGroup

  import SquashedRow._

  def group: LocalityGroup = agentState.group

  def cache(v: Seq[Observation]): this.type = {
    agentState.rollout.cache(v)
    this
  }

  def uncache: this.type = {
    agentState.rollout.unCache
    this
  }

//  def explodeData(
//      field: Field, // TODO: changed to Resolved[Any]
//      ordinalKey: Field,
//      forkType: ForkType,
//      sampler: Sampler[Any]
//  ): SquashedRow = {
//
//    val newRows = dataRows.flatMap { row =>
//      val newDataRows = row.self.explode(
//        field,
//        ordinalKey,
//        forkType,
//        sampler
//      )
//      val newRows = newDataRows.map { dd =>
//        row.copy(self = dd)
//      }
//      newRows
//    }
//
//    val result = this.copy(dataRows = newRows)
//
//    result
//  }

  def flatMapData[DD](
      fn: Data.WithScope[D] => Seq[Data.WithScope[DD]]
  ): SquashedRow[DD] = {

    val newDataRows = dataSeq.flatMap { row =>
      val newRows = fn(row)
      newRows
    }

    this.copy(dataSeq = newDataRows)
  }

//  def remove(fields: Field*): SquashedRow = {
//
//    val newRows = dataRows.map { row =>
//      row.copy(self = row.self.--(fields))
//    }
//
//    this.copy(
//      dataRows = newRows
//    )
//  }

  def exploring: SquashedRow[Data.Exploring[D]] = {
    this.copy(
      dataSeq = {
        dataSeq.map { d =>
          d.copy(
            data = Data.Exploring(d.data).idRefresh
          )
        }
      }
    )
  }

  case class _WithCtx(spooky: SpookyContext) extends NOTSerializable {

    lazy val withDefaultScope: SquashedRow[D] = {

      lazy val uids = group.withCtx(spooky).trajectory.map(_.uid)

      val newDataRows = dataSeq.map { row =>
        row.copy(scopeUIDs = uids)
      }

      SquashedRow.this.copy(dataSeq = newDataRows)
    }

    lazy val unSquash: Seq[FetchedRow[D]] = {

      lazy val lookup = group.withCtx(spooky).lookup

      dataSeq.map { r1 =>
        val scopeUID = r1.scopeUIDs
        val inScope = scopeUID.map { uid =>
          lookup(uid)
        }

        FetchedRow(r1.data, inScope, r1.ordinal)
      }
    }

    def applyDelta[O](
        fn: ChainPlan.Fn[D, O]
    ): SquashedRow[O] = {

      val newDataRows: Seq[Data.WithScope[O]] = unSquash.flatMap { row: FetchedRow[D] =>
        val newRows = fn(row)
        newRows
      }

      SquashedRow.this.copy(dataSeq = newDataRows)
    }

    def fetch(fn: FetchedRow[D] => TraceSet): Seq[(Trace, D)] = {

      val result = unSquash.flatMap { row: FetchedRow[D] =>
        val traces = fn(row)

        val result = traces.map { trace =>
          trace -> row.data
        }

        result
      }

      result
    }
  }

  @transient lazy val withSchema: Fn[SpookySchema, WithSchema[D]] = Impl { v =>
    WithSchema(this, v)
  }
}
