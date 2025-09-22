package com.tribbloids.spookystuff.row

import ai.acyclic.prover.commons.function.hom.Hom.:=>
import ai.acyclic.prover.commons.spark.serialization.NOTSerializable
import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.doc.Observation
import com.tribbloids.spookystuff.execution.ExecutionContext
import com.tribbloids.spookystuff.execution.ExplorePlan.State

object SquashedRow {

  def ofData[D](data: D*): SquashedRow[D] = {

    SquashedRow(
      LocalityGroup.noOp,
      data.zipWithIndex
    )
  }
}

case class SquashedRow[D](
    localityGroup: LocalityGroup,
    batch: Seq[(D, Int)]
) extends SpookyContext.Contextual {
  // can only support 1 agent
  // will trigger a fork if not all agent actions were captured by the LocalityGroup

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
          batch.map { t =>
            val result = Data.Exploring(t._1).startLineage
            result -> t._2
          }
        }
      )
    }
  }

  implicit final class _WithCtx(ctx: SpookyContext) extends NOTSerializable {

    lazy val unSquash: Seq[AgentRow[D]] = {

      batch.map { t =>
        AgentRow(localityGroup, t._1, t._2, ExecutionContext(ctx))
      }
    }
  }

  implicit class _WithSchema(
      schema: SpookySchema
  ) {
    // empty at the moment, but SpookySchema may be extended later

    val withCtx = SquashedRow.this.withCtx(schema.ctx)
  }

  @transient lazy val withSchema: :=>[SpookySchema, _WithSchema] = :=>.at { v =>
    new _WithSchema(v)
  }
    .cached()

}
