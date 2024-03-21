package com.tribbloids.spookystuff.row

import com.tribbloids.spookystuff.actions.Wget
import com.tribbloids.spookystuff.testutils.SpookyBaseSpec

/**
  * Created by peng on 05/04/16.
  */
class SquashedRowSuite extends SpookyBaseSpec {

  it("execution yields at least 1 trajectory") {
    val row = FetchedRow.blank.squash
    val grouped = row.dataSeq.map(_.scopeUIDs)
    assert(grouped == Seq(Seq()))
  }

  it("['a 'b 'a 'b].splitByDistinctNames yields ['a 'b] ['a 'b]") {
    def wget = Wget(HTML_URL)

    val trace = wget ~ 'a +>
      wget ~ 'b +>
      wget ~ 'a +>
      wget ~ 'b
    val row1 = FetchedRow(observations = trace.fetch(spooky)).squash

    val row = row1
      .flatMap(_.splitByDistinctNames)

    val groupedNames = row.dataSeq.map { dataRow =>
      dataRow.scopeUIDs
        .map {
          _.name
        }
        .mkString("")
    }

    assert(groupedNames == Seq("ab", "ab"))
  }
}
