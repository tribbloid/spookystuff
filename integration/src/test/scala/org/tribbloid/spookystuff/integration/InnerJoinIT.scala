package org.tribbloid.spookystuff.integration

import org.tribbloid.spookystuff.{dsl, SpookyContext}
import org.tribbloid.spookystuff.actions.Visit
import dsl._

/**
 * Created by peng on 12/5/14.
 */
class InnerJoinIT extends IntegrationSuite {
  override def doMain(spooky: SpookyContext): Unit = {

    import spooky._

    val base = noInput
      .fetch(
        Visit("http://webscraper.io/test-sites/e-commerce/allinone")
      )

    val joined = base
      .join($"div.sidebar-nav a", indexKey = 'i1)(
        Visit('A.href),
        joinType = Inner
      )(
        'A.text > 'category
      )
      .join($"a.subcategory-link", indexKey = 'i2)(
        Visit('A.href),
        joinType = Inner
      )(
        'A.text > 'subcategory
      )
      .select($"h1".text > 'header)
      .asSchemaRDD()

    assert(
      joined.schema.fieldNames ===
        "i1" ::
          "category" ::
          "i2" ::
          "subcategory" ::
          "header" :: Nil
    )

    val rows = joined.collect()
    assert(rows.size === 3)

    assert(rows(0).mkString("|") === "1|Computers|0|Laptops|Computers / Laptops")
    assert(rows(1).mkString("|") === "1|Computers|1|Tablets|Computers / Tablets")
    assert(rows(2).mkString("|") === "2|Phones|0|Touch|Phones / Touch")
  }

  override def numPages: Int = 6
}
