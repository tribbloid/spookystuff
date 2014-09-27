package org.tribbloid.spookystuff.integration.price

import org.tribbloid.spookystuff.entity.client._
import org.tribbloid.spookystuff.integration.TestCore

/**
 * Created by peng on 14/08/14.
 */
object Macys extends TestCore {

  import spooky._

import scala.concurrent.duration._

  override def doMain() = {
    ((noInput
      +> Wget("http://www.macys.com/")
      !=!())
      .wgetJoin("div#globalMastheadCategoryMenu li a")()
      .extract(
        "category" -> (_.text1("div#nav_title"))
      )
      .visit("div#localNavigationContainer li.nav_cat_item_bold a")() +>
      DelayFor("ul#thumbnails li.productThumbnail").in(20.seconds) +>
      Snapshot() +>
      Loop(
        Click("a.arrowRight")
          :: Delay(5.seconds)
          :: DelayFor("ul#thumbnails li.productThumbnail").in(20.seconds)
          :: Snapshot()
          :: Nil,
        2
      )
      !><())
      .extract(
        "subcategory" -> (_.text1("h1#currentCatNavHeading"))
      )
      .sliceJoin("ul#thumbnails li.productThumbnail")(indexKey = "page")
      .extract(
        "short-description" -> (_.text1("div.shortDescription")),
        "prices" -> (_.text1("div.prices")),
        "rating" -> (_.attr1("div.pdpreviews span.rating span", "style")),
        "reviews" -> (_.text1("div.pdpreviews"))
      )
      .asSchemaRDD()
  }
}