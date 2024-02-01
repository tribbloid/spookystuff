package com.tribbloids.spookystuff.row

import ai.acyclic.prover.commons.function.Impl
import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions.{Trace, TraceSet}
import com.tribbloids.spookystuff.doc.Observation
import com.tribbloids.spookystuff.dsl.ForkType
import com.tribbloids.spookystuff.extractors.Resolved
import com.tribbloids.spookystuff.row.DataRow.WithScope
import com.tribbloids.spookystuff.utils.serialization.NOTSerializable

object SquashedRow {

  def ofData(dataRows: DataRow.WithScope*): SquashedRow = {

    SquashedRow(
      AgentState.ofTrace(Trace.NoOp),
      dataRows
    )
  }

//  def ofDatum(dataRow: DataRow): SquashedRow = ofData(Seq(dataRow))

//  lazy val blank: SquashedRow = {
//    ofData(Seq(DataRow.blank))
//  }
  // TODO: gone, use FetchedRow.toSquashedRow()

  case class WithSchema(
      self: SquashedRow,
      schema: SpookySchema
  ) {

    @transient lazy val withCtx: self._WithCtx = self.withCtx(schema.spooky)

    /**
      * operation is applied per unSquashed row, each row may yield multiple runnable traces
      *
      * this function will never remove a single unSquashed row, in worst case, each row yield an empty trace
      *
      * @param traces
      *   material for interpolation
      * @return
      *   a mapping from runnable trace to data
      */
    def interpolateAndRewrite(
        traces: TraceSet
    ): Seq[(Trace, DataRow)] = {

      val pairs: Seq[(DataRow, TraceSet.NonEmpty)] = withCtx.unSquash.map { row =>
        val raw = traces.flatMap { trace =>
          val rewritten = trace.interpolateAndRewrite(row, schema)
          rewritten
        }

        row.dataRow -> TraceSet(raw).avoidEmpty
      }

      val result = pairs.flatMap {
        case (d, ts) =>
          ts.map(t => t -> d)
      }

      //      val filtered: Seq[(DataRow, Seq[TraceView])] =
      //        if (!filterEmpty) pairs
      //        else {
      //          val result = pairs.filter(_._2.nonEmpty)
      //          result
      //        }

      result
    }
  }
}

case class SquashedRow(
    agentState: AgentState,
    dataRows: Seq[DataRow.WithScope]
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

  def explodeData(
      field: Field, // TODO: changed to Resolved[Any]
      ordinalKey: Field,
      forkType: ForkType,
      sampler: Sampler[Any]
  ): SquashedRow = {

    val newRows = dataRows.flatMap { row =>
      val newDataRows = row.self.explode(
        field,
        ordinalKey,
        forkType,
        sampler
      )
      val newRows = newDataRows.map { dd =>
        row.copy(self = dd)
      }
      newRows
    }

    val result = this.copy(dataRows = newRows)

    result
  }

  def explodeScope(
      fn: DataRow.WithScope => Seq[WithScope]
  ): SquashedRow = {
    // TODO: merge into explodeData after typed field is implemented

    val newDataRows = dataRows.flatMap { row =>
      val newRows = fn(row)
      newRows
    }

    this.copy(dataRows = newDataRows)
  }

  def remove(fields: Field*): SquashedRow = {

    val newRows = dataRows.map { row =>
      row.copy(self = row.self.--(fields))
    }

    this.copy(
      dataRows = newRows
    )
  }

  def withLineageIDs: SquashedRow = {
    this.copy(
      dataRows = {
        dataRows.map { vv =>
          vv.copy(
            self = vv.self.withLineageID
          )
        }
      }
    )
  }

  case class _WithCtx(spooky: SpookyContext) extends NOTSerializable {

    lazy val resetScope: SquashedRow = {

      lazy val uids = group.withCtx(spooky).trajectory.map(_.uid)

      val newDataRows = dataRows.map { row =>
        row.copy(scopeUIDs = uids)
      }

      SquashedRow.this.copy(dataRows = newDataRows)
    }

    lazy val unSquash: Seq[FetchedRow] = {

      lazy val lookup = group.withCtx(spooky).lookup

      dataRows.map { r1 =>
        val scopeUID = r1.scopeUIDs
        val inScope = scopeUID.map { uid =>
          lookup(uid)
        }

        FetchedRow(r1, inScope, r1.ordinal)
      }
    }

    /**
      * the size of dataRows may increase according to the following rules:
      *
      * each dataRow yield {exs.size} x {afterDelta.size} dataRows.
      * @param filterEmptyKeep1Datum
      *   if true, output DataRows with empty KV extraction will be replaced by the original source
      */
    private def extractImpl(
        exs: Seq[Resolved[Any]]
        // TODO: this is useless
    ): SquashedRow = {

      val fetchedRows = this.unSquash

      // each element contains a different page group, CAUTION: not all of them are used: page group that yield no new datum will be removed, if all groups yield no new datum at least 1 row is preserved
      val old_new = {

        val result = fetchedRows.map { fetchedRow =>
          val dataRow = fetchedRow.dataRowWithScope
          val KVOpts: Seq[(Field, Option[Any])] = exs.flatMap { expr =>
            val resolving = expr.field.conflictResolving
            val k = expr.field
            val vOpt = expr.lift.apply(fetchedRow)
            resolving match {
              case Field.Replace => Some(k -> vOpt)
              case _             => vOpt.map(v => k -> Some(v))
            }
          }
          dataRow -> KVOpts
        }

        result
      }

      val newDataRows = old_new.map { tuple =>
        val K_VOrRemoves = tuple._2
        val dataRow = tuple._1
        val newKVs = K_VOrRemoves.collect {
          case (field, Some(v)) => field -> v
        }
        val removeKs = K_VOrRemoves.collect {
          case (field, None) => field
        }
        val updated = dataRow ++ newKVs -- removeKs

        updated
        dataRow.copy(updated)

      }

      SquashedRow.this.copy(dataRows = newDataRows)
    }

    def extract(ex: Resolved[Any]*): SquashedRow = extractImpl(ex)
  }

  @transient lazy val withSchema = Impl { v =>
    WithSchema(this, v)
  }
}
