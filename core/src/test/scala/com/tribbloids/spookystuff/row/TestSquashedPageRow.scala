package com.tribbloids.spookystuff.row

import com.tribbloids.spookystuff.SpookyEnvSuite
import com.tribbloids.spookystuff.actions.{Snapshot, Visit}
import com.tribbloids.spookystuff.doc.Fetched

/**
  * Created by peng on 05/04/16.
  */
class TestSquashedPageRow extends SpookyEnvSuite {

  import com.tribbloids.spookystuff.dsl._

  test("Array[Page]().grouping yields at least 1 group") {
    val row = SquashedFetchedRow(fetchedOpt = Some(Array[Fetched]()))
    val grouped = row.defaultGroupedFetched.toSeq
    assert(grouped == Seq(Seq()))
  }

  test("['a 'b 'a 'b].grouping yields ['a 'b] ['a 'b]") {
    val trace = List(
      Visit(STATIC_WIKIPEDIA_URI),
      Snapshot() ~ 'a,
      Snapshot() ~ 'b,
      Snapshot() ~ 'a,
      Snapshot() ~ 'b
    )
    val row = SquashedFetchedRow(fetchedOpt = Some(trace.fetch(spooky).toArray))
    val grouped = row.defaultGroupedFetched.toSeq
    val groupedNames = grouped.map {
      _.map {
        _.name
      }.mkString("")
    }

    assert(groupedNames == Seq("ab", "ab"))
  }
}
