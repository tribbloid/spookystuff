package org.tribbloid.spookystuff.integration

import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.dsl._

/**
 * Created by peng on 12/5/14.
 */
class LeftJoinIT extends IntegrationSuite {
  override def doMain(spooky: SpookyContext): Unit = {

    import spooky._

    val base = noInput
      .fetch(
        Wget("http://webscraper.io/test-sites/e-commerce/allinone")
      )

    val joined = base
      .join($"div.sidebar-nav a", ordinalKey = 'i1)(
        Wget('A.href),
        joinType = LeftOuter
      )(
        'A.text ~ 'category
      )
      .join($"a.subcategory-link", ordinalKey = 'i2)(
        Wget('A.href),
        joinType = LeftOuter
      )(
        'A.text ~ 'subcategory
      )
      .select($"h1".text ~ 'header)
      .toSchemaRDD()

    assert(
      joined.schema.fieldNames ===
        "i1" ::
          "category" ::
          "i2" ::
          "subcategory" ::
          "header" :: Nil
    )

    val rows = joined.collect()
    assert(rows.size === 4)
    assert(rows(0).mkString("|") === "0|Home|null|null|null")
    assert(rows(1).mkString("|") === "1|Computers|0|Laptops|Computers / Laptops")
    assert(rows(2).mkString("|") === "1|Computers|1|Tablets|Computers / Tablets")
    assert(rows(3).mkString("|") === "2|Phones|0|Touch|Phones / Touch")
  }

  override def numPages = {
    case Narrow => 7
    case _ => 6
  }

  override def numDrivers = _ => 0
}
