package com.tribbloids.spookystuff.row

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions.Foundation.HasTrace
import com.tribbloids.spookystuff.actions.{Mock, NoOp}
import com.tribbloids.spookystuff.doc.Observation
import com.tribbloids.spookystuff.execution.ExecutionContext

object BuildRow {

  def mock[D](
      data: D,
      observations: Seq[Observation]
  ): BuildRow[D] = {

    val mockAction = Mock(observations)

    BuildRow(data, mockAction)
  }

  def mock(
      observations: Seq[Observation]
  ): BuildRow[Unit] = {
    mock((), observations)
  }

  def noOp[D](
      data: D
  ): BuildRow[D] = {
    BuildRow(data)
  }

  lazy val blank: BuildRow[Unit] = BuildRow(())
}

/**
  * just a constructor, need to be cast into [[SquashedRow]] or [[AgentRow]]
  */
case class BuildRow[D](
    data: D,
    hasTrace: HasTrace = NoOp
) {

  def squashed: SquashedRow[D] = {
    SquashedRow(
      LocalityGroup(hasTrace.trace),
      Seq(data -> 0)
    )
  }

  def fetched(ctx: SpookyContext): AgentRow[D] = {
    AgentRow(
      LocalityGroup(hasTrace.trace),
      data,
      0,
      ExecutionContext(ctx)
    )
  }
}
