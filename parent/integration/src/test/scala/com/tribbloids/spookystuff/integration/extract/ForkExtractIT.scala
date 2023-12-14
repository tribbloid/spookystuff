package com.tribbloids.spookystuff.integration.extract

import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.integration.ITBaseSpec

/**
  * Created by peng on 11/26/14.
  */
class ForkExtractIT extends ITBaseSpec {

  override lazy val driverFactories = Seq(
    null
  )

  override def doMain(): Unit = {

    val raw = spooky
      .fetch(
        Wget(HTML_URL)
      )
      .fork(S"div.central-featured-lang")(
        'A.attr("lang"),
        A"a".href,
        A"a em".text,
        'A.uri
      )

    val result = raw
      .toDF(sort = true)

    result.schema.treeString.shouldBe(
      """
        |root
        | |-- _c1: string (nullable = true)
        | |-- _c2: string (nullable = true)
        | |-- _c3: string (nullable = true)
        | |-- _c4: string (nullable = true)
      """.stripMargin
    )

    val rows = result.collect()

    assert(rows.length === 10)
    assert(rows.head.size === 4)
    assert(rows.head.getString(0) === "en")
    assert(rows.head.getString(1) contains "en.wikipedia.org/")
    assert(rows.head.getString(2) === "The Free Encyclopedia")
    assert(rows.head.getString(3) contains "testutils/files/Wikipedia.html")
  }

  override def numPages: Long = 1
}
