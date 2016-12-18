package com.tribbloids.spookystuff.row

import com.tribbloids.spookystuff.actions.Wget
import com.tribbloids.spookystuff.testutils.LocalPathDocsFixture
import com.tribbloids.spookystuff.{SpookyEnvFixture, dsl}

import scala.language.implicitConversions

/**
  * Created by peng on 12/3/14.
  */
class FetchedRowViewSuite extends SpookyEnvFixture with LocalPathDocsFixture {

  import dsl._

  test("get page") {
    val pages = (
      Wget(HTML_URL) :: Nil
      ).fetch(spooky)
    val row = FetchedRow(fetched = pages)

    val page1 = row.getOnlyDoc
    assert(page1.get === pages.head)

    println(Wget(HTML_URL).toString())
    val defaultName = Wget(HTML_URL).toString()
    val page2 = row.getDoc(defaultName)
    assert(page2.get === pages.head)
  }

  test("get unstructured") {
    val pages = (
      (Wget(HTML_URL) as 'pp) :: Nil
      ).fetch(spooky)
    val row = FetchedRow(fetched = pages)
      .squash(spooky)
      .extract(
        S("h1.central-textlogo img").head withAlias 'e1,
        'pp.findAll("label") withAlias 'lang
      )
      .unsquash.head

    val page2 = row.getUnstructured('pp)
    assert(page2.get === pages.head)

    val e1 = row.getUnstructured('e1)
    assert(e1.get.attr("title").get === "Wikipedia")

    val e2 = row.getUnstructured('lang)
    assert(e2.get.text.get contains "language")
  }
}