package com.tribbloids.spookystuff.row

import com.tribbloids.spookystuff.actions.{Trace, TraceSet}
import com.tribbloids.spookystuff.doc.{Fetched, Trajectory}
import com.tribbloids.spookystuff.dsl.ForkType
import com.tribbloids.spookystuff.extractors.Resolved

import java.util.UUID

object BottleneckRow {

  def apply(data: Map[Field, Any]): BottleneckRow = BottleneckRow(
    dataRows = Vector(DataRow(data))
  )

  lazy val empty: BottleneckRow = apply(Map.empty[Field, Any])
}

/**
  * the main data structure in execution plan, chosen for its very small serialization footprint (hence the name
  * "Bottleneck")
  *
  * consisting of several dataRows from the previous stage, a shared traceView, and a function representing all agent
  * actions and data transformations, Observations are deliberately omitted for being too large and will slow down
  * shipping
  *
  * for access concrete data & observations, use SquashedRow or FetchedRow
  */
case class BottleneckRow(
    dataRows: Vector[DataRow] = Vector(),
    traceView: Trace = Trace(),
    delta: SquashedRow => Seq[SquashedRow] = v => Seq(v)
) {

  def setCache(
      vs: Seq[Fetched] = null
  ): this.type = {
    this.traceView.setCache(vs)
    this
  }

  private object AndThen {

    def flatMap(next: SquashedRow => Seq[SquashedRow]): BottleneckRow = {

      val newDelta = delta.andThen { vs =>
        val result = vs.flatMap(next)
        result
      }

      BottleneckRow.this.copy(
        delta = newDelta
      )
    }

    def map(next: SquashedRow => SquashedRow): BottleneckRow = flatMap(v => Seq(next(v)))
  }

  def explodeData(
      field: Field,
      ordinalKey: Field,
      forkType: ForkType,
      sampler: Sampler[Any]
  ): BottleneckRow = AndThen.map { state =>
    val newDataRows = state.dataRows.flatMap { row =>
      row.explode(field, ordinalKey, forkType, sampler)
    }

    state copy (
      dataRows = newDataRows
    )
  }

  def explodeTrajectory(
      fn: Trajectory => Seq[Trajectory]
  ): BottleneckRow = AndThen.flatMap { state =>
    val newTrajectories = fn(state.trajectory)
    newTrajectories.map { tt =>
      state.copy(
        trajectory = tt
      )
    }
  }

  def remove(fields: Field*): BottleneckRow = AndThen.map { state =>
    state.copy(
      dataRows = state.dataRows.map(_.--(fields))
    )
  }

  /**
    * the size of dataRows may increase according to the following rules:
    *
    * each dataRow yield {exs.size} x {afterDelta.size} dataRows.
    * @param filterEmptyKeep1Datum
    *   if true, output DataRows with empty KV extraction will be replaced by the original source
    */
  private def extractImpl(
      exs: Seq[Resolved[Any]],
      filterEmptyKeep1Datum: Boolean = true
  ): BottleneckRow = AndThen.map { state =>
    val fetchedRows = state.unSquashed
    // each element contains a different page group, CAUTION: not all of them are used: page group that yield no new datum will be removed, if all groups yield no new datum at least 1 row is preserved
    val old_new = fetchedRows.map { pageRow =>
      val dataRow = pageRow.dataRow
      val KVOpts: Seq[(Field, Option[Any])] = exs.flatMap { expr =>
        val resolving = expr.field.conflictResolving
        val k = expr.field
        val vOpt = expr.lift.apply(pageRow)
        resolving match {
          case Field.Replace => Some(k -> vOpt)
          case _             => vOpt.map(v => k -> Some(v))
        }
      }
      dataRow -> KVOpts
    }

    val filtered =
      if (!filterEmptyKeep1Datum) old_new
      else {
        val filtered = old_new.filter(_._2.exists(_._2.nonEmpty))
        if (filtered.isEmpty) old_new.headOption.toVector // leave at least 1 DataRow with empty KV output
        else filtered
      }

    val updatedDataRows = filtered.map { tuple =>
      val K_VOrRemoves = tuple._2
      val dataRow = tuple._1
      val newKVs = K_VOrRemoves.collect {
        case (field, Some(v)) => field -> v
      }
      val removeKs = K_VOrRemoves.collect {
        case (field, None) => field
      }
      val updatedDataRow = dataRow ++ newKVs -- removeKs

      updatedDataRow
    }

    state.copy(dataRows = updatedDataRows)
  }

  def extract(ex: Resolved[Any]*): BottleneckRow = extractImpl(ex)

  case class WSchema(
      schema: SpookySchema
  ) extends Serializable {

    lazy val withSpooky: traceView.WithSpooky =
      new BottleneckRow.this.traceView.WithSpooky(schema.spooky)

    lazy val raw: SquashedRow = {
      val dataRowsWithLineageIDs = dataRows.map { row =>
        row.copy(
          exploreLineageID = Some(UUID.randomUUID())
        )
      }

      SquashedRow(dataRowsWithLineageIDs, Trajectory.raw(withSpooky.observations))
    }

    lazy val deltaApplied: Seq[SquashedRow] = {
      val result = delta(raw)
      result
    }

    lazy val unsquashed: Seq[FetchedRow] = deltaApplied.flatMap(_.unSquashed)

    def interpolateAndRewrite(
        traces: TraceSet
    ): Seq[(Trace, DataRow)] = { // TODO: should be DataRow -> TraceView, keep the same order!

      val result = deltaApplied.flatMap { state =>
        state.interpolateAndRewrite(traces, schema)
      }

      result
    }
  }
}
