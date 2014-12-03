package org.tribbloid.spookystuff.sparkbinding

import java.text.SimpleDateFormat

import org.tribbloid.spookystuff.SparkEnvSuite
import org.tribbloid.spookystuff.actions.Wget
import org.tribbloid.spookystuff.dsl._

/**
 * Created by peng on 11/26/14.
 */
class TestPageRowRDD extends SparkEnvSuite {

  test("flatMap inherited from RDD") {
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

  test("select") {
    import spooky._

    val rdd = noInput
      .fetch(
        Wget("http://www.wikipedia.org/")
      )
      .select(
        '*.uri,
        '*.timestamp, //TODO:'$.saved,
        $"div.central-featured-lang".texts
      )

    val schemaRDD = rdd
      .asSchemaRDD()

    val fields = schemaRDD.schema.fieldNames

    assert(
      fields ===
        "*_uri" ::
          "*_timestamp" ::
          "*_children(div_central-featured-lang)_texts" :: Nil
    )

    val finishedTime = System.currentTimeMillis()

    val res = schemaRDD.collect()

    assert(res.size === 1)

    val head = res.head
    assert(head.size === 3)
    assert(head.getString(0) === "http://www.wikipedia.org/")
    val parsedTime = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(head.getString(1)).getTime
    assert(parsedTime < finishedTime)
    assert(parsedTime > finishedTime-2000)
    val texts = head.apply(2).asInstanceOf[Iterable[String]]
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
        'A.attr("lang"),
        A"a".href,
        A"a em".text,
        '*.uri,
        'A.uri
      )

    val schemaRDD = rdd
      .asSchemaRDD()

    val fields = schemaRDD.schema.fieldNames

    assert(
      fields ===
        "A_attr(lang,true)" ::
          "A_children(a)_head_attr(abs:href,true)" ::
          "A_children(a em)_head_text" ::
          "*_uri" ::
          "A_uri" :: Nil
    )

    val res = schemaRDD.collect()

    assert(res.size === 10)
    assert(res.head.size === 5)
    assert(res.head.getString(0) === "en")
    assert(res.head.getString(1) === "http://en.wikipedia.org/")
    assert(res.head.getString(2) === "The Free Encyclopedia")
    assert(res.head.getString(3) === "http://www.wikipedia.org/")
    assert(res.head.getString(4) === "http://www.wikipedia.org/")
  }
}