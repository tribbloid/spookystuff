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

    val snapshotAllPages = (
      Snapshot() ~ 'first
        +> Loop (
        ClickNext("button.btn", "1"::Nil)
          +> Delay(2.seconds)
          +> Snapshot()
      ))

    val base = noInput
      .fetch(
        Visit("http://webscraper.io/test-sites/e-commerce/ajax")
          +> snapshotAllPages
      )

    val result = base
      .explore($"div.sidebar-nav a", depthKey = 'depth, checkpointInterval = 2)(
        Visit('A.href)
          +> snapshotAllPages,
        flattenPagesIndexKey = 'page_index
      )(
        $"button.btn.btn-primary".text ~ 'page_number,
        'A.text ~ 'category,
        'first.children("h1").text ~ 'title,
        $"a.title".size ~ 'num_product
      )
      .toSchemaRDD()

    assert(
      result.schema.fieldNames ===
        "depth" ::
          "page_index" ::
          "page_number" ::
          "category" ::
          "title" ::
          "num_product" :: Nil
    )

    val rows = result.collect().map(_.mkString("|"))
    assert(rows.size === 12)

    assert(rows contains "0|null|null|null|E-commerce training site|3")//TODO: add back multi-tier index key
    assert(rows contains "1|0|null|Computers|Computers category|3")
    assert(rows contains "1|0|null|Phones|Phones category|3")
    assert(rows contains "2|0|1|Laptops|Computers / Laptops|6")
    assert(rows contains "2|1|2|Laptops|null|6")
    assert(rows contains "2|2|3|Laptops|null|6")
    assert(rows contains "2|0|1|Touch|Phones / Touch|6")
    assert(rows contains "2|0|1|Tablets|Computers / Tablets|6")
    assert(rows contains "2|1|2|Touch|null|6")
    assert(rows contains "2|1|2|Tablets|null|6")
    assert(rows contains "2|2|3|Tablets|null|6")
    assert(rows contains "2|3|4|Tablets|null|6")
  }

  override def numSessions = _ => 6

  override def numPages = _ => 12
}
