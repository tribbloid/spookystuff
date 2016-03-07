package com.tribbloids.spookystuff.integration.select

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.integration.IntegrationSuite

/**
  * Created by peng on 11/26/14.
  */
class FlatSelectIT extends IntegrationSuite {

  override lazy val drivers = Seq(
    null
  )

  override def doMain(spooky: SpookyContext) {

    val raw = spooky
      .fetch(
        Wget("http://www.wikipedia.org/")
      )
      .flatExtract(S"div.central-featured-lang")(
        'A.attr("lang"),
        A"a".href,
        A"a em".text,
        'A.uri
      )

    val result = raw
      .toDF(sort = true)

    assert(
      result.schema.fieldNames ===
        "A_attr(lang,true)" ::
          "A_findAll(a)_href" ::
          "A_findAll(a em)_text" ::
          "A_uri" :: Nil
    )

    val rows = result.collect()

    assert(rows.length === 10)
    assert(rows.head.size === 4)
    assert(rows.head.getString(0) === "en")
    assert(rows.head.getString(1) contains "en.wikipedia.org/")
    assert(rows.head.getString(2) === "The Free Encyclopedia")
    assert(rows.head.getString(3) contains "www.wikipedia.org/")
  }

  override def numFetchedPages = _ => 1

  override def numDrivers = 0
}