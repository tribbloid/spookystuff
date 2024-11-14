package com.tribbloids.spookystuff.row

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions.{HasTrace, Mock, Trace}
import com.tribbloids.spookystuff.doc.Observation

object Row {

  def mock[D](
      data: D,
      observations: Seq[Observation]
  ): Row[D] = {

    val mockAction = Mock(observations)

    Row(data, mockAction)
  }

  def mock(
      observations: Seq[Observation]
  ): Row[Unit] = {
    mock((), observations)
  }

  def noOp[D](
      data: D
  ): Row[D] = {
    Row(data)
  }

  lazy val blank: Row[Unit] = Row(())
}

/**
  * just a constructor, need to be cast into [[SquashedRow]] or [[FetchedRow]]
  */
case class Row[D](
    data: D,
    hasTrace: HasTrace = Trace.NoOp
) {

  def asSquashed: SquashedRow[D] = {
    SquashedRow(
      LocalityGroup(hasTrace.asTrace)(),
      Seq(data)
    )
  }

  def asFetched(ctx: SpookyContext): FetchedRow[D] = {
    FetchedRow(
      agentState = AgentState.Real(LocalityGroup(hasTrace.asTrace)(), ctx),
      data = data
    )
  }
}
