package com.tribbloids.spookystuff.row

import ai.acyclic.prover.commons.function.hom.Hom.:=>
import ai.acyclic.prover.commons.spark.serialization.NOTSerializable
import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.doc.Observation
import com.tribbloids.spookystuff.execution.ExecutionContext
import com.tribbloids.spookystuff.execution.ExplorePlan.State

import scala.language.implicitConversions

object SquashedRow {

  def ofData[D](data: D*): SquashedRow[D] = {

    SquashedRow(
      LocalityGroup.noOp,
      data
    )
  }
}

case class SquashedRow[D](
    localityGroup: LocalityGroup,
    batch: Seq[D]
) extends SpookyContext.Contextual {
  // can only support 1 agent
  // will trigger a fork if not all agent actions were captured by the LocalityGroup

  import SquashedRow.*

  def cache(v: Seq[Observation]): this.type = {
    localityGroup.rollout.cache(v)
    this
  }

  def uncache: this.type = {
    localityGroup.rollout.uncache
    this
  }

  object exploring {

    def state0[O]: (LocalityGroup, State[D, O]) = {

      localityGroup -> State(
        row0 = Some(SquashedRow.this),
        open = None,
        visited = None
      )
    }

    def startLineage: SquashedRow[Data.Exploring[D]] = {
      SquashedRow.this.copy(
        batch = {
          batch.map { d =>
            val result = Data.Exploring(d).startLineage
            result
          }
        }
      )
    }
  }

  case class _WithCtx(ctx: SpookyContext) extends NOTSerializable {

    lazy val unSquash: Seq[FetchedRow[D]] = {

      batch.map { d =>
        FetchedRow(localityGroup, d, ExecutionContext(ctx))
      }
    }

  }

  class WithSchema(
      schema: SpookySchema
  ) extends _WithCtx(schema.ctx) {
    // empty at the moment, but SpookySchema may be extended later
  }

  @transient lazy val withSchema: :=>[SpookySchema, WithSchema] = :=>.at { v =>
    new WithSchema(v)
  }

  override def withCtx(v: SpookyContext): _WithCtx = _WithCtx(v)
}
