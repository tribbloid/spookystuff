package com.tribbloids.spookystuff.integration.select

import com.tribbloids.spookystuff.QueryException
import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.integration.IntegrationSuite

class SelectIT extends IntegrationSuite {

  override def doMain() {

    val set = spooky
      .fetch(
        Visit(HTML_URL)
      )
      .select(
        S.uri,
        S.timestamp,
        //        S"div.central-featured-lang".head ~ 'element,
        //        S"div.central-featured-lang" ~ 'elements,
        S"div.central-featured-lang em".text ~ 'title,
        S"div.central-featured-lang strong".texts ~ 'langs,
        S"a.link-box em".expand(-2 to 1).texts ~ 'expanded
      )
      .persist()

    val df = set
      .toDF(sort = true)

    df.schema.treeString.shouldBe(
      """
        |root
        | |-- _c1: string (nullable = true)
        | |-- _c2: timestamp (nullable = true)
        | |-- title: string (nullable = true)
        | |-- langs: array (nullable = true)
        | |    |-- element: string (containsNull = true)
        | |-- expanded: array (nullable = true)
        | |    |-- element: string (containsNull = true)
      """.stripMargin
    )

    val rows = df.collect()
    val finishTime = System.currentTimeMillis()
    assert(rows.length === 1)
    assert(rows.head.size === 5)
    assert(rows.head.getString(0) contains "spookystuff/test/Wikipedia.html")
    val parsedTime = rows.head.getTimestamp(1).getTime
    assert(parsedTime < finishTime +2000) //due to round-off error
    assert(parsedTime > finishTime-60000) //long enough even after the second time it is retrieved from the cache
    val title = rows.head.getString(2)
    assert(title === "The Free Encyclopedia")
    val langs = rows.head(3).asInstanceOf[Iterable[String]]
    assert(langs.size === 10)
    assert(langs.head === "English")
    val expanded = rows.head(4).asInstanceOf[Iterable[String]]
    assert(expanded.size === 10)
    assert(expanded.head === "English The Free Encyclopedia")

    intercept[QueryException] {
      set.select(
        S"div.central-featured-lang strong".text ~ 'title
      )
    }

    val rdd2 = set
      .select(
        S"div.central-featured-lang strong".text ~+ 'title
      )

    val df2 = rdd2
      .toDF(sort = true)

    df2.schema.treeString.shouldBe(

    )

    val rows2 = df2.collect()
    val titles = rows2.head.getAs[Iterable[String]]("title")
    assert(titles === Seq("The Free Encyclopedia", "English"))
  }

  override def numPages= 1
}
