package org.tribbloid.spookystuff.example.forum

import org.tribbloid.spookystuff.entity.Wget
import org.tribbloid.spookystuff.SpookyContext._
import org.tribbloid.spookystuff.example.SparkSubmittable

/**
 * Created by peng on 20/08/14.
 */
object RottenTomatoes extends SparkSubmittable {
  override def doMain(): Unit = {
    (sc.parallelize(Seq("Dummy")) +>
      Wget("http://www.rottentomatoes.com/") !)
      .joinBySlice("div#boxoffice tbody tr")
      .select(
        "rating" -> (_.ownText1("tr td.titleColumn").replaceAll("\"","").trim),
        "name" -> (_.text1("tr td.titleColumn a")),
        "year" -> (_.text1("tr td.titleColumn span")),
        "box_weekend" -> (_.text("tr td.ratingColumn")(0)),
        "box_gross" -> (_.text("td.ratingColumn")(1)),
        "weeks" -> (_.text1("tr td.weeksColumn"))
      )
  }
}
