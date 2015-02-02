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
      .explore($"ul.pagination a", depthKey = 'depth)(
        Wget('A.href)
      )(
        'A.text ~ 'page
      )
      .select($.uri ~ 'uri)
      .toSchemaRDD()

    assert(
      result.schema.fieldNames ===
        "depth" ::
          "page" ::
          "uri" :: Nil
    )

    val rows = result.collect().map(_.mkString("|"))
    assert(rows.size === 5)

    assert(rows contains "0|null|http://webscraper.io/test-sites/e-commerce/static/computers/tablets")
    assert(rows contains "1|2|http://webscraper.io/test-sites/e-commerce/static/computers/tablets/2")
    assert(rows contains "1|3|http://webscraper.io/test-sites/e-commerce/static/computers/tablets/3")
    assert(rows contains "1|4|http://webscraper.io/test-sites/e-commerce/static/computers/tablets/4")
    assert(rows contains "2|1|http://webscraper.io/test-sites/e-commerce/static/computers/tablets/1")
  }

  override def numPages = _ => 5

  override def numDrivers = _ => 0
}
