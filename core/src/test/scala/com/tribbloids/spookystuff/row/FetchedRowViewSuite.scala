package com.tribbloids.spookystuff.row

import com.tribbloids.spookystuff.actions.Wget
import com.tribbloids.spookystuff.{SpookyEnvSuite, dsl}

import scala.language.implicitConversions

/**
  * Created by peng on 12/3/14.
  */
class FetchedRowViewSuite extends SpookyEnvSuite {

  import dsl._
  import com.tribbloids.spookystuff.utils.Implicits._

  test("get page") {
    val pages = (
      Wget(STATIC_WIKIPEDIA_URI) :: Nil
      ).fetch(spooky)
    val row = FetchedRow(pageLikes = pages)

    val page1 = row.getOnlyPage
    assert(page1.get === pages.head)

    println(Wget(STATIC_WIKIPEDIA_URI).toString())
    val page2 = row.getPage(Wget(STATIC_WIKIPEDIA_URI).toString())
    assert(page2.get === pages.head)
  }

  test("get unstructured") {
    val pages = (
      (Wget(STATIC_WIKIPEDIA_URI) as 'pp) :: Nil
      ).fetch(spooky)
    val row = FetchedRow(pageLikes = pages)
      .squash
      .extract(
        S("h1.central-textlogo img").head named 'e1,
        'pp.findAll("label") named 'lang
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