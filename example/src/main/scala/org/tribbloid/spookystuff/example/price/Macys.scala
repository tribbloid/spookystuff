package org.tribbloid.spookystuff.example.price

import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.dsl._
import org.tribbloid.spookystuff.example.ExampleCore

import scala.concurrent.duration._

/**
 * Created by peng on 14/08/14.
 */
object Macys extends ExampleCore {

  override def doMain(spooky: SpookyContext) = {
    import spooky._

    noInput
      .fetch(
        Wget("http://www.macys.com/")
      )
      .wgetJoin($"div#globalMastheadCategoryMenu li a")
      .select(
        $"div#nav_title".text > 'category
      )
      .join($"div#localNavigationContainer li.nav_cat_item_bold a")(
        Visit('A)
          +> WaitFor("ul#thumbnails li.productThumbnail").in(20.seconds)
          +> Snapshot()
          +> Loop(
          Click("a.arrowRight")
            :: Delay(5.seconds)
            :: WaitFor("ul#thumbnails li.productThumbnail").in(20.seconds)
            :: Snapshot()
            :: Nil,
          2
        )
      )()
      .select(
        $"h1#currentCatNavHeading".text > 'subcategory
      )
      .flatSelect($"ul#thumbnails li.productThumbnail", indexKey = 'page)(
        A"div.shortDescription".text > 'short_description,
        A"div.prices".text > 'prices,
        A"div.pdpreviews span.rating span".attr("style") > 'rating,
        A"div.pdpreviews".text > 'reviews
      )
      .asSchemaRDD()
  }
}