package com.tribbloids.spookystuff.row

import com.tribbloids.spookystuff.SpookyEnvSuite
import com.tribbloids.spookystuff.actions.{ActionLike, Snapshot, Trace, Visit}
import com.tribbloids.spookystuff.doc.Fetched
import org.apache.spark.sql.catalyst.ScalaReflection

/**
  * Created by peng on 05/04/16.
  */
class SquashedFetchedRowSuite extends SpookyEnvSuite {

  import com.tribbloids.spookystuff.dsl._

  test("Array[Page]().grouping yields at least 1 group") {
    val row = SquashedFetchedRow(_fetched = Array[Fetched]())
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
    val row = SquashedFetchedRow(_fetched = trace.fetch(spooky).toArray)
    val grouped = row.defaultGroupedFetched.toSeq
    val groupedNames = grouped.map {
      _.map {
        _.name
      }.mkString("")
    }

    assert(groupedNames == Seq("ab", "ab"))
  }
}
