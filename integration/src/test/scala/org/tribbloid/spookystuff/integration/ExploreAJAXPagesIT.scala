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
          +> Snapshot().as('first)
          +> Loop (
          ClickNext("button.btn", "1"::Nil) :: Delay(2.seconds) :: Snapshot() :: Nil
        )
      )

    val result = base
      .explore($"div.sidebar-nav a", depthKey = 'depth, indexKey = 'i1)(
        Visit('A.href)
          +> Snapshot().as('first)
          +> Loop (
          ClickNext("button.btn", "1"::Nil) :: Delay(2.seconds) :: Snapshot() :: Nil
        ),
        flattenPagesIndexKey = 'page
      )(
        $"button.btn.btn-primary".text > 'real_page,
        'A.text > 'category,
        'first.children("h1").text > 'title,
        $"a.title".size > 'num_product
      )
      .asSchemaRDD()

    assert(
      result.schema.fieldNames ===
        "depth" ::
          "i1" ::
          "page" ::
          "real_page" ::
          "category" ::
          "title" ::
          "num_product" :: Nil
    )

    val rows = result.collect()
    assert(rows.size === 12)
  }

  override def numSessions = 6

  override def numPages: Int = 12

  override def numDrivers = 6
}
