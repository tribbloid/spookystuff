package com.tribbloids.spookystuff.row

import com.tribbloids.spookystuff.actions.Wget
import com.tribbloids.spookystuff.testutils.SpookyBaseSpec

/**
  * Created by peng on 05/04/16.
  */
class BottleneckRowSuite extends SpookyBaseSpec {

  it("execution yields at least 1 trajectory") {
    val row = BottleneckRow()
    val grouped = row.deltaApplied.map(_.trajectory.inScope)
    assert(grouped == Seq(Seq()))
  }

  it("['a 'b 'a 'b].splitByDistinctNames yields ['a 'b] ['a 'b]") {
    def wget = Wget(HTML_URL)

    val trace = wget ~ 'a +>
      wget ~ 'b +>
      wget ~ 'a +>
      wget ~ 'b
    val row =
      BottleneckRow().setCache(trace.fetch(spooky)).explodeTrajectory(_.splitByDistinctNames)
    val states = row.deltaApplied
    val groupedNames = states.map { state =>
      state.trajectory
        .map {
          _.name
        }
        .mkString("")
    }

    assert(groupedNames == Seq("ab", "ab"))
  }
}
