package com.tribbloids.spookystuff.row

import com.tribbloids.spookystuff.actions.Wget
import com.tribbloids.spookystuff.testutils.{LocalPathDocsFixture, SpookyBaseSpec}
import com.tribbloids.spookystuff.dsl

/**
  * Created by peng on 12/3/14.
  */
class FetchedRowSuite extends SpookyBaseSpec with LocalPathDocsFixture {

  import dsl._

  it("get page") {
    val pages = Wget(HTML_URL).fetch(spooky)
    val row = FetchedRow(trajectory = pages)

    val page1 = row.getOnlyDoc
    assert(page1.get === pages.head)

    println(Wget(HTML_URL).toString)
    val defaultName = Wget(HTML_URL).toString
    val page2 = row.getDoc(defaultName)
    assert(page2.get === pages.head)
  }

  it("get unstructured") {
    val pages = (Wget(HTML_URL) as 'pp).fetch(spooky)
    val row = FetchedRow(trajectory = pages)
      .asBottleneckRow()
      .extract(
        S("h1.central-textlogo img").head withAlias 'e1,
        'pp.findAll("label") withAlias 'lang
      )
      .unsquashed
      .head

    val page2 = row.getUnstructured('pp)
    assert(page2.get === pages.head.root)

    val e1 = row.getUnstructured('e1)
    assert(e1.get.attr("title").get === "Wikipedia")

    val e2 = row.getUnstructured('lang)
    assert(e2.get.text.get contains "language")
  }
}
