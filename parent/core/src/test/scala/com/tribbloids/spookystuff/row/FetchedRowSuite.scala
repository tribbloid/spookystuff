package com.tribbloids.spookystuff.row

import com.tribbloids.spookystuff.actions.Wget
import com.tribbloids.spookystuff.doc.{Elements, Unstructured}
import com.tribbloids.spookystuff.testutils.{FileDocsFixture, SpookyBaseSpec}

/**
  * Created by peng on 12/3/14.
  */
class FetchedRowSuite extends SpookyBaseSpec {

  val resources: FileDocsFixture.type = FileDocsFixture
  import resources.*

  it("get only doc") {
    val doc = Wget(HTML_URL).fetch(spooky).head
    val row = Row((), Wget(HTML_URL)).asFetched(spooky)

    val page1 = row.docs.only
    assert(page1.uid === doc.uid)

    val defaultName = Wget(HTML_URL).toString
    val page2 = row.docs.ofName(defaultName).only
    assert(page2.uid === doc.uid)
  }

  it("get unstructured") {
    val wget = Wget(HTML_URL) as "pp"
    val doc = wget.fetch(spooky).head
    val proto = Row((), wget).asFetched(spooky)

    val row = {
      Seq(proto).select { row =>
        new Object {
          val e1: Unstructured = row.docs.\("h1.central-textlogo img").head
          val lang: Elements[Unstructured] = row.docs.\("label")
        }
      }.head
    }

    val doc2 = row.docs("pp").only
    assert(doc2.root === doc.root)

    val e1 = row.data.e1
    assert(e1.attr("title").get === "Wikipedia")

    val e2 = row.data.lang
    assert(e2.text.get contains "language")
  }

  describe("rescope") {

    it("['a 'b 'a 'b] ~> ['a 'b] ['a 'b]") {

      val wget = Wget(HTML_URL)

      val trace = wget ~ "a" +>
        wget ~ "b" +>
        wget ~ "a" +>
        wget ~ "b"
      val row1 = Row((), hasTrace = trace).asFetched(spooky)

      val rows: Seq[Data.Scoped[Unit]] = row1.rescope.byDistinctNames

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
