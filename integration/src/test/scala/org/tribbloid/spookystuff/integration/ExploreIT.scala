package org.tribbloid.spookystuff.integration

import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.actions.Visit
import org.tribbloid.spookystuff.dsl._

/**
 * Created by peng on 12/5/14.
 */
class ExploreIT extends IntegrationSuite {
  override def doMain(spooky: SpookyContext): Unit = {

    import spooky._

    val base = noInput
      .fetch(
        Visit("http://webscraper.io/test-sites/e-commerce/allinone")
      )

    val explored = base
      .explore($"div.sidebar-nav a", depthKey = 'depth, indexKey = 'i1)(
        Visit('A.href),
        flattenPagesIndexKey = 'page
      )(
        'A.text ~ 'category
      )
      .select($"h1".text ~ 'header)
      .asSchemaRDD()

    assert(
      explored.schema.fieldNames ===
        "depth" ::
          "i1" ::
          "page" ::
          "category" ::
          "header" :: Nil
    )

    val rows = explored.collect()
    assert(rows.size === 6)

    assert(rows(0).mkString("|") === "0|null|null|null|E-commerce training site")
    assert(rows(1).mkString("|") === "1|1|0|Computers|Computers category")
    assert(rows(2).mkString("|") === "1|2|0|Phones|Phones category")
    assert(rows(3).mkString("|") === "2|2|0|Laptops|Computers / Laptops")
    assert(rows(4).mkString("|") === "2|3|0|Tablets|Computers / Tablets")
    assert(rows(5).mkString("|") === "2|3|0|Touch|Phones / Touch")
  }

  override def numPages: Int = 6
}