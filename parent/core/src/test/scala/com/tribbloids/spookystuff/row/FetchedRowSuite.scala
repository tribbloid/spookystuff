package com.tribbloids.spookystuff.row

import com.tribbloids.spookystuff.actions.{Trace, Wget}
import com.tribbloids.spookystuff.doc.{Elements, Unstructured}
import com.tribbloids.spookystuff.testutils.{FileDocsFixture, SpookyBaseSpec}

/**
  * Created by peng on 12/3/14.
  */
class FetchedRowSuite extends SpookyBaseSpec with FileDocsFixture {

  it("get page") {
    val pages = Wget(HTML_URL).fetch(spooky)
    val row = Row.mock(data = (), observations = pages).asFetched(spooky)

    val page1 = row.docs.only
    assert(page1.ofName === pages.head)

    println(Wget(HTML_URL).toString)
    val defaultName = Wget(HTML_URL).toString
    val page2 = row.docs.ofName(defaultName).only
    assert(page2.ofName === pages.head)
  }

  it("get unstructured") {
    val docs = (Wget(HTML_URL) as 'pp).fetch(spooky)
    val proto = Row
      .mock(observations = docs)
      .asFetched(spooky)

    val row = {
      Seq(proto).select { row =>
        new Object {
          val e1: Unstructured = row.docs.\("h1.central-textlogo img").head
          val lang: Elements[Unstructured] = row.docs.\("label")
        }
      }.head

    }

    val page2 = row.docs("pp")
    assert(page2.head.unbox === docs.head.root)

    val e1 = row.data.e1
    assert(e1.attr("title").get === "Wikipedia")

    val e2 = row.data.lang
    assert(e2.text.get contains "language")
  }

  describe("rescope") {

    it("byDistinctNames: ['a 'b 'a 'b] ~> ['a 'b] ['a 'b]") {

      def wget = Wget(HTML_URL)

      val trace = wget ~ 'a +>
        wget ~ 'b +>
        wget ~ 'a +>
        wget ~ 'b
      val row1 = Row(trace).asFetched(spooky)

      val rows: Seq[Data.Scoped[Trace]] = row1.rescope.byDistinctNames

      val groupedNames = rows.map { row =>
        row.scope.observationUIDs
          .map {
            _.name
          }
          .mkString("")
      }

      assert(groupedNames == Seq("ab", "ab"))

    }
  }
}
