package com.tribbloids.spookystuff.row

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.execution.Delta
import com.tribbloids.spookystuff.utils.serialization.NOTSerializable

object DeltaRowLike {

  case class SrcRow(
      self: SquashedRow
  ) extends DeltaRowLike {}

  case class DeltaRow(
      prev: DeltaRow,
      delta: Delta
  ) extends DeltaRowLike

  /**
    * @param schema
    *   CAUTION! this is the schema after all delta has been applied it will not match dataRos in raw
    */
  case class WithSchema(
      row: DeltaRow,
      schema: SpookySchema
  ) {}
}

/**
  * the main data structure in execution plan, chosen for its very small serialization footprint (a.k.a. "Bottleneck")
  *
  * consisting of several dataRows from the previous stage, a shared traceView, and a function representing all agent
  * actions and data transformations, Observations are deliberately omitted for being too large and will slow down
  * shipping
  *
  * for access concrete data & observations, use DeltaRow or FetchedRow
  */
sealed trait DeltaRowLike extends SpookyContext.CanRunWith {
  // TODO: just a scaffold, not used

  case class _WithCtx(spookyContext: SpookyContext) extends NOTSerializable {}
}
