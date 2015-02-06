package org.tribbloid.spookystuff.integration

import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.dsl._

/**
 * Created by peng on 12/10/14.
 */
class ExploreNextPageIT extends IntegrationSuite {

  override def doMain(spooky: SpookyContext): Unit = {
    import spooky._

    val result = noInput
      .fetch(
        Wget("http://webscraper.io/test-sites/e-commerce/static/computers/tablets")
      )
      .explore($"ul.pagination a[rel=next]", depthKey = 'page)(
        Wget('A.href)
      )()
      .select($.uri ~ 'uri)
      .toSchemaRDD()

    assert(
      result.schema.fieldNames ===
        "page" ::
          "uri" :: Nil
    )

    val rows = result.collect().map(_.mkString("|"))
    assert(rows.size === 4)

    assert(rows contains "0|http://webscraper.io/test-sites/e-commerce/static/computers/tablets")
    assert(rows contains "1|http://webscraper.io/test-sites/e-commerce/static/computers/tablets/2")
    assert(rows contains "2|http://webscraper.io/test-sites/e-commerce/static/computers/tablets/3")
    assert(rows contains "3|http://webscraper.io/test-sites/e-commerce/static/computers/tablets/4")
//    assert(rows contains "2|1|http://webscraper.io/test-sites/e-commerce/static/computers/tablets/1")
  }

  override def numPages = _ => 4

  override def numDrivers = _ => 0
}
