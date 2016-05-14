package com.tribbloids.spookystuff.integration.select

import java.text.SimpleDateFormat

import com.tribbloids.spookystuff.QueryException
import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.dsl._
import com.tribbloids.spookystuff.integration.IntegrationSuite

class SelectIT extends IntegrationSuite {

  override def doMain() {

    val pageRowRDD = spooky
      .fetch(
        Visit("http://www.wikipedia.org/")
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

      val df = pageRowRDD
      .toDF(sort = true)

    assert(
      df.schema.fieldNames ===
        "_c1" ::
          "_c2" ::
          "title" ::
          "langs" ::
          "expanded" :: Nil
    )

    val rows = df.collect()
    val finishTime = System.currentTimeMillis()
    assert(rows.length === 1)
    assert(rows.head.size === 5)
    assert(rows.head.getString(0) contains "://www.wikipedia.org/")
    val parsedTime = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(rows.head.getString(1)).getTime
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
      pageRowRDD.select(
        S"div.central-featured-lang strong".text ~ 'title
      )
    }

    val RDD2 = pageRowRDD
      .select(
        S"div.central-featured-lang strong".text ~+ 'title
      )
      .toDF(sort = true)

    assert(
      RDD2.schema.fieldNames ===
        "_c1" ::
          "_c2" ::
          "title" ::
          "langs" ::
          "expanded" :: Nil
    )

    val rows2 = RDD2.collect()
    val titles = rows2.head(2).asInstanceOf[Iterable[String]]
    assert(titles === Seq("The Free Encyclopedia", "English"))
  }

  override def numPages= 1
}