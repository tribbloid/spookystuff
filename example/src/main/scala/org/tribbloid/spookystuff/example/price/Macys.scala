package org.tribbloid.spookystuff.example.price

import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.expressions._
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
      .wgetJoin('* href "div#globalMastheadCategoryMenu li a")()
      .extract(
        "category" -> (_.text1("div#nav_title"))
      )
      .join('* href "div#localNavigationContainer li.nav_cat_item_bold a" as '~)(
        Visit("#{~}")
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
      )
      .extract(
        "subcategory" -> (_.text1("h1#currentCatNavHeading"))
      )
      .sliceJoin("ul#thumbnails li.productThumbnail")(indexKey = 'page)
      .extract(
        "short-description" -> (_.text1("div.shortDescription")),
        "prices" -> (_.text1("div.prices")),
        "rating" -> (_.attr1("div.pdpreviews span.rating span", "style")),
        "reviews" -> (_.text1("div.pdpreviews"))
      )
      .asSchemaRDD()
  }
}