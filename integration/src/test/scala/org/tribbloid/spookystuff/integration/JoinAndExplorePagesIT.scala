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
        'A.text > 'category
      )
      .join($"a.subcategory-link", indexKey = 'i2)(
        Wget('A.href),
        joinType = LeftOuter
      )(
        'A.text > 'subcategory
      )
      .select($"h1".text > 'header)

    val result = joined
      .explore($"ul.pagination a", depthKey = 'depth, indexKey = 'i3)(
        Wget('A.href)
      )(
        'A.text as 'page
      )
      .select($.uri > 'uri)
      .asSchemaRDD()
      .persist()

    assert(
      result.schema.fieldNames ===
        "i1" ::
          "category" ::
          "i2" ::
          "subcategory" ::
          "header" ::
          "depth" ::
          "i3" ::
          "page" ::
          "uri" :: Nil
    )

    val rows = result.collect()
    assert(rows.size === 13)

    assert(rows(0).mkString("|") === "0|Home|null|null|null|0|null|null|null")
    assert(rows(1).mkString("|") === "1|Computers|0|Laptops|Computers / Laptops|0|null|null|http://webscraper.io/test-sites/e-commerce/static/computers/laptops")
    assert(rows(2).mkString("|") === "1|Computers|0|Laptops|Computers / Laptops|1|0|2|http://webscraper.io/test-sites/e-commerce/static/computers/laptops/2")
    assert(rows(3).mkString("|") === "1|Computers|0|Laptops|Computers / Laptops|1|1|3|http://webscraper.io/test-sites/e-commerce/static/computers/laptops/3")
    assert(rows(4).mkString("|") === "1|Computers|0|Laptops|Computers / Laptops|2|1|1|http://webscraper.io/test-sites/e-commerce/static/computers/laptops/1")
    assert(rows(5).mkString("|") === "1|Computers|1|Tablets|Computers / Tablets|0|null|null|http://webscraper.io/test-sites/e-commerce/static/computers/tablets")
    assert(rows(6).mkString("|") === "1|Computers|1|Tablets|Computers / Tablets|1|0|2|http://webscraper.io/test-sites/e-commerce/static/computers/tablets/2")
    assert(rows(7).mkString("|") === "1|Computers|1|Tablets|Computers / Tablets|1|1|3|http://webscraper.io/test-sites/e-commerce/static/computers/tablets/3")
    assert(rows(8).mkString("|") === "1|Computers|1|Tablets|Computers / Tablets|1|2|4|http://webscraper.io/test-sites/e-commerce/static/computers/tablets/4")
    assert(rows(9).mkString("|") === "1|Computers|1|Tablets|Computers / Tablets|2|0|«|http://webscraper.io/test-sites/e-commerce/static/computers/tablets/1")
    assert(rows(10).mkString("|") === "2|Phones|0|Touch|Phones / Touch|0|null|null|http://webscraper.io/test-sites/e-commerce/static/phones/touch")
    assert(rows(11).mkString("|") === "2|Phones|0|Touch|Phones / Touch|1|0|2|http://webscraper.io/test-sites/e-commerce/static/phones/touch/2")
    assert(rows(12).mkString("|") === "2|Phones|0|Touch|Phones / Touch|2|0|«|http://webscraper.io/test-sites/e-commerce/static/phones/touch/1")
  }

  override def numPages: Int = 15
}
