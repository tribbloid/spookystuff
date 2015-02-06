package org.tribbloid.spookystuff.integration

import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.dsl._

/**
 * Created by peng on 12/10/14.
 */
class JoinAndExplorePagesIT extends IntegrationSuite {

  override def doMain(spooky: SpookyContext): Unit = {
    import spooky._

    val joined = noInput
      .fetch(
        Wget("http://webscraper.io/test-sites/e-commerce/static")
      )
      .join($"div.sidebar-nav a", indexKey = 'i1)(
        Wget('A.href),
        joinType = LeftOuter
      )(
        'A.text ~ 'category
      )
      .join($"a.subcategory-link", indexKey = 'i2)(
        Wget('A.href),
        joinType = LeftOuter
      )(
        'A.text ~ 'subcategory
      )
      .select($"h1".text ~ 'header)

    val result = joined
      .explore($"ul.pagination a", depthKey = 'depth)(
        Wget('A.href)
      )(
        'A.text as 'page
      )
      .select($.uri ~ 'uri)
      .toSchemaRDD()
      .persist()

    assert(
      result.schema.fieldNames ===
        "i1" ::
          "category" ::
          "i2" ::
          "subcategory" ::
          "header" ::
          "depth" ::
          "page" ::
          "uri" :: Nil
    )

    val rows = result.collect().map(_.mkString("|"))
    assert(rows.size === 13)

    assert(rows contains "0|Home|null|null|null|0|null|null")
    assert(rows contains "1|Computers|0|Laptops|Computers / Laptops|0|null|http://webscraper.io/test-sites/e-commerce/static/computers/laptops")
    assert(rows contains "1|Computers|0|Laptops|Computers / Laptops|1|2|http://webscraper.io/test-sites/e-commerce/static/computers/laptops/2")
    assert(rows contains "1|Computers|0|Laptops|Computers / Laptops|1|3|http://webscraper.io/test-sites/e-commerce/static/computers/laptops/3")
//    assert(rows contains "1|Computers|0|Laptops|Computers / Laptops|2|1|http://webscraper.io/test-sites/e-commerce/static/computers/laptops/1")
    assert(rows contains "1|Computers|1|Tablets|Computers / Tablets|0|null|http://webscraper.io/test-sites/e-commerce/static/computers/tablets")
    assert(rows contains "1|Computers|1|Tablets|Computers / Tablets|1|2|http://webscraper.io/test-sites/e-commerce/static/computers/tablets/2")
    assert(rows contains "1|Computers|1|Tablets|Computers / Tablets|1|3|http://webscraper.io/test-sites/e-commerce/static/computers/tablets/3")
    assert(rows contains "1|Computers|1|Tablets|Computers / Tablets|1|4|http://webscraper.io/test-sites/e-commerce/static/computers/tablets/4")
//    assert(rows contains "1|Computers|1|Tablets|Computers / Tablets|2|1|http://webscraper.io/test-sites/e-commerce/static/computers/tablets/1")
    assert(rows contains "2|Phones|0|Touch|Phones / Touch|0|null|http://webscraper.io/test-sites/e-commerce/static/phones/touch")
    assert(rows contains "2|Phones|0|Touch|Phones / Touch|1|2|http://webscraper.io/test-sites/e-commerce/static/phones/touch/2")
//    assert(rows contains "2|Phones|0|Touch|Phones / Touch|2|«|http://webscraper.io/test-sites/e-commerce/static/phones/touch/1")
  }

  override def numPages = {
    case Minimal => 16
    case _ => 15
  }

  override def numDrivers = _ => 0
}
