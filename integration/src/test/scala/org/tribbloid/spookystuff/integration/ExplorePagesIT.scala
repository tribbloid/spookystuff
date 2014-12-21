package org.tribbloid.spookystuff.integration

import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.dsl._

/**
 * Created by peng on 12/10/14.
 */
class ExplorePagesIT extends IntegrationSuite {

  override def doMain(spooky: SpookyContext): Unit = {
    import spooky._

    val result = noInput
      .fetch(
        Wget("http://webscraper.io/test-sites/e-commerce/static/computers/tablets")
      )
      .explore($"ul.pagination a", depthKey = 'depth, indexKey = 'idx)(
        Wget('A.href)
      )(
        'A.text ~ 'page
      )
      .select($.uri ~ 'uri)
      .asSchemaRDD()

    assert(
      result.schema.fieldNames ===
        "depth" ::
          "idx" ::
          "page" ::
          "uri" :: Nil
    )

    val rows = result.collect()
    assert(rows.size === 5)

    assert(rows(0).mkString("|") === "0|null|null|http://webscraper.io/test-sites/e-commerce/static/computers/tablets")
    assert(rows(1).mkString("|") === "1|0|2|http://webscraper.io/test-sites/e-commerce/static/computers/tablets/2")
    assert(rows(2).mkString("|") === "1|1|3|http://webscraper.io/test-sites/e-commerce/static/computers/tablets/3")
    assert(rows(3).mkString("|") === "1|2|4|http://webscraper.io/test-sites/e-commerce/static/computers/tablets/4")
    assert(rows(4).mkString("|") === "2|0|«|http://webscraper.io/test-sites/e-commerce/static/computers/tablets/1")
  }

  override def numPages = 5

  override def numDrivers = 0
}
