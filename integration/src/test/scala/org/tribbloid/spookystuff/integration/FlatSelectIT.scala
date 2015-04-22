package org.tribbloid.spookystuff.integration

import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.dsl._

/**
 * Created by peng on 11/26/14.
 */
class FlatSelectIT extends IntegrationSuite {

  override lazy val drivers = Seq(
    null
  )

  override def doMain(spooky: SpookyContext) {

    import spooky.dsl._

    val result = spooky
      .fetch(
        Wget("http://www.wikipedia.org/")
      )
      .flatSelect($"div.central-featured-lang")(
        'A.attr("lang"),
        A"a".href,
        A"a em".text,
        'A.uri
      )
      .toDataFrame()

    assert(
      result.schema.fieldNames ===
        "A_attr(lang,true)" ::
          "A_children(a)_attr(abs:href,true)" ::
          "A_children(a em)_text" ::
          "A_uri" :: Nil
    )

    val rows = result.collect()

    assert(rows.size === 10)
    assert(rows.head.size === 4)
    assert(rows.head.getString(0) === "en")
    assert(rows.head.getString(1) === "http://en.wikipedia.org/")
    assert(rows.head.getString(2) === "The Free Encyclopedia")
    assert(rows.head.getString(3) === "http://www.wikipedia.org/")
  }

  override def numPages = _ => 1

  override def numDrivers = _ => 0
}