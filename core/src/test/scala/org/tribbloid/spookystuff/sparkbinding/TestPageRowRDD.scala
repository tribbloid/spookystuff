package org.tribbloid.spookystuff.sparkbinding

import org.tribbloid.spookystuff.SparkEnvSuite
import org.tribbloid.spookystuff.actions.Wget
import org.tribbloid.spookystuff.dsl._

/**
 * Created by peng on 11/26/14.
 */
class TestPageRowRDD extends SparkEnvSuite {

  test("inherited RDD operation") {
    import spooky._

    val rdd = noInput
      .fetch(
        Wget("http://www.wikipedia.org/")
      )
      .select(
        $"div.central-featured-lang".texts > '~
      )

    val texts = rdd
      .flatMap(_.get("~").get.asInstanceOf[Seq[String]])
      .collect()

    assert(texts.size === 10)
    assert(texts.head.startsWith("English"))
  }

  test("flatSelect") {

    import spooky._

    val rdd = noInput
      .fetch(
        Wget("http://www.wikipedia.org/")
      )
      .flatSelect($"div.central-featured-lang")(
        'A.attr("lang") as 'lang,
        A"a".href as 'link,
        A"a em".text as 'sub
      )
      .asSchemaRDD()

    val res = rdd.collect()

    assert(res.size === 10)
    assert(res.head.size === 3)
    assert(res.head.getString(0) === "en")
    assert(res.head.getString(1) === "http://en.wikipedia.org/")
    assert(res.head.getString(2) === "The Free Encyclopedia")
  }
}