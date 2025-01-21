package com.tribbloids.spookystuff.row

import ai.acyclic.prover.commons.spark.serialization.NOTSerializable
import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.execution.Delta

@deprecated("scaffold in an abandoned architecture, don't used")
object DeltaRowLike {

  case class SrcRow[D](
      self: SquashedRow[D]
  ) extends DeltaRowLike[D] {}

  case class DeltaRow[I, O](
      prev: DeltaRowLike[I],
      delta: Delta[I, O]
  ) extends DeltaRowLike[O]

  /**
    * @param schema
    *   CAUTION! this is the schema after all delta has been applied it will not match dataRos in raw
    */
  case class WithSchema[O](
      row: DeltaRowLike[O],
      schema: SpookySchema
  ) {}
}

sealed trait DeltaRowLike[O] extends SpookyContext.Contextual {

  case class _WithCtx(spookyContext: SpookyContext) extends NOTSerializable {}

  override def withCtx(v: SpookyContext): _WithCtx = _WithCtx(v)
}
