package org.tribbloid.spookystuff.example.largescale

import org.tribbloid.spookystuff.SpookyContext._
import org.tribbloid.spookystuff.entity._
import org.tribbloid.spookystuff.example.{SparkRunnable, SparkSubmittable}

/**
 * Created by peng on 14/08/14.
 */
object Macys extends SparkRunnable {

  override def doMain(): Unit = {
    ((sc.parallelize(Seq("Dummy")) +>
      Wget("http://www.macys.com/")!
      ).wgetJoin(
        "div#globalMastheadCategoryMenu li a"
      ).select(
        "category" -> (_.text1("div#nav_title"))
      ).visit("div#localNavigationContainer li.nav_cat_item_bold a") +>
      Snapshot() +>
      Loop(500) {
        Click("a.arrowRight")
        Snapshot()
      }!).map(_.text("div.shortDescription")).collect().foreach(println(_))

//      .join(
//
//      ).saveAs(
//        dir = "file:///home/peng/spookystuff/temp/spooky-page/macys"
//      ).map(_.context.get("category")).collect().foreach(println(_))
  }
}

//.map(_.href("div#localNavigationContainer li.nav_cat_item_bold a")).collect().foreach(println(_))
//