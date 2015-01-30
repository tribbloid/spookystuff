package org.tribbloid.spookystuff.integration

import java.text.SimpleDateFormat

import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.dsl._

/**
 * Created by peng on 11/26/14.
 */
class SelectIT extends IntegrationSuite {

  override def doMain(spooky: SpookyContext) {

    import spooky._

    val RDD = noInput
      .fetch(
        Visit("http://www.wikipedia.org/")
      )
      .select(
        $.uri,
        $.timestamp, //TODO:'$.saved,
        $"div.central-featured-lang".texts ~ '~
      )
      .persist()

    val texts = RDD
      .flatMap(_.get("~").get.asInstanceOf[Seq[String]])
      .collect()

    assert(texts.size === 10)
    assert(texts.head.startsWith("English"))

    val result = RDD.asSchemaRDD()

    assert(
      result.schema.fieldNames ===
        "$_uri" ::
          "$_timestamp" ::
          "~" :: Nil
    )

    val rows = result.collect()
    val finishTime = System.currentTimeMillis()
    assert(rows.size === 1)
    assert(rows.head.size === 3)
    assert(rows.head.getString(0) === "http://www.wikipedia.org/")
    val parsedTime = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(rows.head.getString(1)).getTime
    assert(parsedTime < finishTime +2000) //due to round-off error
    assert(parsedTime > finishTime-60000) //long enough even after the second time it is retrieved from the cache
    val texts2 = rows.head.apply(2).asInstanceOf[Iterable[String]]
    assert(texts2.size === 10)
    assert(texts2.head.startsWith("English"))

    intercept[AssertionError] {
      RDD.select(
        $"title".text ~ '~
      )
    }
  }

  override def numPages = _ => 1
}