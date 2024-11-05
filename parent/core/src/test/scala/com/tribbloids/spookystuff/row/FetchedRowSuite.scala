package com.tribbloids.spookystuff.row

import com.tribbloids.spookystuff.actions.Wget
import com.tribbloids.spookystuff.testutils.{FileDocsFixture, SpookyBaseSpec}

/**
  * Created by peng on 12/3/14.
  */
class FetchedRowSuite extends SpookyBaseSpec with FileDocsFixture {

  it("get page") {
    val pages = Wget(HTML_URL).fetch(spooky)
    val row = FetchedRow(data = (), observations = pages)

    val page1 = row.docs.only
    assert(page1.get === pages.head)

    println(Wget(HTML_URL).toString)
    val defaultName = Wget(HTML_URL).toString
    val page2 = row.docs.get(defaultName).only
    assert(page2.get === pages.head)
  }

  it("get unstructured") {
    val pages = (Wget(HTML_URL) as 'pp).fetch(spooky)
    val row = FetchedRow(observations = pages).squash
      .extract(
        S("h1.central-textlogo img").head withAlias 'e1,
        'pp.findAll("label") withAlias 'lang
      )
      .unSquash
      .head

    val page2 = row.getUnstructured('pp)
    assert(page2.get === pages.head.root)

    val e1 = row.getUnstructured('e1)
    assert(e1.get.attr("title").get === "Wikipedia")

    val e2 = row.getUnstructured('lang)
    assert(e2.get.text.get contains "language")
  }
}
