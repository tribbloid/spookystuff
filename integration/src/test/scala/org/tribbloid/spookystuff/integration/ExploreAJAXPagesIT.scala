package org.tribbloid.spookystuff.integration

import org.tribbloid.spookystuff.{dsl, SpookyContext}
import org.tribbloid.spookystuff.actions._
import dsl._
import scala.concurrent.duration
import duration._

/**
* Created by peng on 12/10/14.
*/
class ExploreAJAXPagesIT extends IntegrationSuite {
  override def doMain(spooky: SpookyContext): Unit = {
    import spooky._

    val base = noInput
      .fetch(
        Visit("http://webscraper.io/test-sites/e-commerce/ajax")
          +> Snapshot() ~ 'first
          +> Loop (
          ClickNext("button.btn", "1"::Nil) :: Delay(2.seconds) :: Snapshot() :: Nil
        )
      )

    val result = base
      .explore($"div.sidebar-nav a", depthKey = 'depth, indexKey = 'i1)(
        Visit('A.href)
          +> Snapshot() ~ 'first
          +> Loop (
          ClickNext("button.btn", "1"::Nil) :: Delay(2.seconds) :: Snapshot() :: Nil
        ),
        flattenPagesIndexKey = 'page_index
      )(
        $"button.btn.btn-primary".text ~ 'page_number,
        'A.text ~ 'category,
        'first.children("h1").text ~ 'title,
        $"a.title".size ~ 'num_product
      )
      .asSchemaRDD()

    assert(
      result.schema.fieldNames ===
        "depth" ::
          "i1" ::
          "page_index" ::
          "page_number" ::
          "category" ::
          "title" ::
          "num_product" :: Nil
    )

    val rows = result.collect()
    assert(rows.size === 12)

    rows.foreach{
      row => println(row.mkString("|"))
    }

    assert(rows(0).mkString("|") === "0|null|null|null|null|E-commerce training site|3")
    assert(rows(1).mkString("|") === "1|1|0|null|Computers|Computers category|3")
    assert(rows(2).mkString("|") === "1|2|0|null|Phones|Phones category|3")
    assert(rows(3).mkString("|") === "2|2|0|1|Laptops|Computers / Laptops|6")
    assert(rows(4).mkString("|") === "2|2|1|2|Laptops|null|6")
    assert(rows(5).mkString("|") === "2|2|2|3|Laptops|null|6")
    assert(rows(6).mkString("|") === "2|3|0|1|Touch|Phones / Touch|6")
    assert(rows(7).mkString("|") === "2|3|0|1|Tablets|Computers / Tablets|6")
    assert(rows(8).mkString("|") === "2|3|1|2|Touch|null|6")
    assert(rows(9).mkString("|") === "2|3|1|2|Tablets|null|6")
    assert(rows(10).mkString("|") === "2|3|2|3|Tablets|null|6")
    assert(rows(11).mkString("|") === "2|3|2|3|Tablets|null|6")
  }

  override def numSessions = 6

  override def numPages: Int = 12

  override def numDrivers = 6
}
