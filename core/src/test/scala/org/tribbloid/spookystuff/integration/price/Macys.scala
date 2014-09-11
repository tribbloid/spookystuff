package org.tribbloid.spookystuff.integration.price

import org.apache.spark.rdd.RDD
import org.tribbloid.spookystuff.entity.clientaction._
import org.tribbloid.spookystuff.entity.clientaction.Loop
import org.tribbloid.spookystuff.integration.SpookyTestCore

/**
 * Created by peng on 14/08/14.
 */
object Macys extends SpookyTestCore {

  import spooky._

  override def doMain() = {
    ((sc.parallelize(Seq("Dummy"))
      +> Wget("http://www.macys.com/")
      !=!())
      .wgetJoin("div#globalMastheadCategoryMenu li a")()
      .extract(
        "category" -> (_.text1("div#nav_title"))
      )
      .visit("div#localNavigationContainer li.nav_cat_item_bold a")() +>
      DelayFor("ul#thumbnails li.productThumbnail",20).canFail() +>
      Snapshot() +>
      Loop(2) (
        Click("a.arrowRight"),
        Delay(5),
        DelayFor("ul#thumbnails li.productThumbnail",20),
        Snapshot()
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