package com.tribbloids.spookystuff.row

import com.tribbloids.spookystuff.actions.{Trace, TraceSet}
import com.tribbloids.spookystuff.doc.Trajectory

case class SquashedRow(
    dataRows: Vector[DataRow],
    trajectory: Trajectory
) {

  @transient lazy val unSquashed: Vector[FetchedRow] = {

    dataRows.map(d => FetchedRow(d, trajectory))
  }

  /**
    * operation is applied per unSquashed row, each row may yield multiple runnable traces
    *
    * this function will never remove a single unSquashed row, in worst case, each row yield an empty trace
    *
    * @param traces
    *   material for interpolation
    * @param deduplicate
    *   if true, remove duplicated interpolated traces for each unSquashed row
    * @return
    *   a mapping from runnable trace to data
    */
  def interpolateAndRewrite(
      traces: TraceSet,
      schema: SpookySchema
  ): Seq[(Trace, DataRow)] = {

    val pairs: Seq[(DataRow, TraceSet.NonEmpty)] = unSquashed.map { row =>
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
