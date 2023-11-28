package com.tribbloids.spookystuff.row

import com.tribbloids.spookystuff.actions.Wget
import com.tribbloids.spookystuff.testutils.SpookyBaseSpec

/**
  * Created by peng on 05/04/16.
  */
class SquashedFetchedRowSuite extends SpookyBaseSpec {

  it("Array[Page]().grouping yields at least 1 group") {
    val row = SquashedFetchedRow()
    val grouped = row.fetchedSlices.toSeq
    assert(grouped == Seq(Seq()))
  }

  it("['a 'b 'a 'b].grouping yields ['a 'b] ['a 'b]") {
    def wget = Wget(HTML_URL)

    val trace = wget ~ 'a +>
      wget ~ 'b +>
      wget ~ 'a +>
      wget ~ 'b
    val row = SquashedFetchedRow.withDocs(docs = trace.fetch(spooky))
    val grouped = row.fetchedSlices.toSeq
    val groupedNames = grouped.map {
      _.map {
        _.name
      }.mkString("")
    }

    assert(groupedNames == Seq("ab", "ab"))
  }
}
