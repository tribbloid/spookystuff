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
      .explore($"div.sidebar-nav a", depthKey = 'depth)(
        Visit('A.href),
        flattenPagesOrdinalKey = 'page
      )(
        'A.text ~ 'category
      )
      .select($"h1".text ~ 'header)
      .toSchemaRDD()

    assert(
      explored.schema.fieldNames ===
        "depth" ::
          "page" ::
          "category" ::
          "header" :: Nil
    )

    val rows = explored.collect().map(_.mkString("|"))
    assert(rows.size === 6)

    assert(rows contains "0|null|null|E-commerce training site")
    assert(rows contains "1|0|Computers|Computers category")
    assert(rows contains "1|0|Phones|Phones category")
    assert(rows contains "2|0|Laptops|Computers / Laptops")
    assert(rows contains "2|0|Tablets|Computers / Tablets")
    assert(rows contains "2|0|Touch|Phones / Touch")
  }

  override def numPages = _ => 6
}