package com.tribbloids.spookystuff.row

import com.tribbloids.spookystuff.SpookyEnvFixture
import com.tribbloids.spookystuff.actions.{Snapshot, Visit}

/**
  * Created by peng on 05/04/16.
  */
class SquashedFetchedRowSuite extends SpookyEnvFixture {

  import com.tribbloids.spookystuff.dsl._

  test("Array[Page]().grouping yields at least 1 group") {
    val row = SquashedFetchedRow()
    val grouped = row.defaultGroupedFetched.toSeq
    assert(grouped == Seq(Seq()))
  }

  test("['a 'b 'a 'b].grouping yields ['a 'b] ['a 'b]") {
    val trace = List(
      Visit(HTML_URL),
      Snapshot() ~ 'a,
      Snapshot() ~ 'b,
      Snapshot() ~ 'a,
      Snapshot() ~ 'b
    )
    val row = SquashedFetchedRow.withDocs(docs = trace.fetch(spooky))
    val grouped = row.defaultGroupedFetched.toSeq
    val groupedNames = grouped.map {
      _.map {
        _.name
      }.mkString("")
    }

    assert(groupedNames == Seq("ab", "ab"))
  }
}
