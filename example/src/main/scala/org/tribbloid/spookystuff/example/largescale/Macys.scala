package org.tribbloid.spookystuff.example.largescale

import org.tribbloid.spookystuff.SpookyContext._
import org.tribbloid.spookystuff.entity._
import org.tribbloid.spookystuff.example.SparkSubmittable

/**
 * Created by peng on 14/08/14.
 */
object Macys extends SparkSubmittable {

  override def doMain(): Unit = {
    ((sc.parallelize(Seq("Dummy")) +>
      Wget("http://www.macys.com/")!==
      ).wgetJoin(
        "div#globalMastheadCategoryMenu li a"
      ).selectInto(
        "category" -> (_.text1("div#nav_title"))
      ).visit("div#localNavigationContainer li.nav_cat_item_bold a") +>
      Loop(1) (DelayFor("ul#thumbnails li.productThumbnail",20)) +>
      Snapshot() +>
      Loop(2) (
        Click("a.arrowRight"),
        Delay(5),
        DelayFor("ul#thumbnails li.productThumbnail",20),
        Snapshot()
      )!><).selectInto(
        "subcategory" -> (_.text1("h1#currentCatNavHeading"))
      ).joinBySlice(
        "ul#thumbnails li.productThumbnail"
      ).map(page => (
      page.backtrace.size,
      page.context.get("category"),
      page.context.get("subcategory"),
      page.text1("div.shortDescription"),
      page.text1("div.prices"),
      page.attr1("div.pdpreviews span.rating span", "style"),
      page.text1("div.pdpreviews")
      ).productIterator.toList.mkString("\t")
      ).collect().foreach(println(_))

//      map(page => (
//      page.backtrace.size,
//      page.context.get("category"),
//      page.context.get("subcategory"),
//      page.text("ul#thumbnails div.shortDescription")
//      ).productIterator.toList.mkString("\t")
//      ).collect().foreach(println(_))

//      .join(
//
//      ).saveAs(
//        dir = "file:///home/peng/spookystuff/temp/spooky-page/macys"
//      ).map(_.context.get("category")).collect().foreach(println(_))
  }
}

//.map(_.href("div#localNavigationContainer li.nav_cat_item_bold a")).collect().foreach(println(_))
//