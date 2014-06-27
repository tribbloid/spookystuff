package org.tribbloid.spookystuff.example

import org.tribbloid.spookystuff.SpookyContext._
import org.tribbloid.spookystuff.entity._

/**
 * Created by peng on 18/06/14.
 */
object ResellerRatings extends SparkSubmittable {

  def doMain() {

    (sc.parallelize(Seq("Hewlett_Packard")) +>
      Wget("http://www.resellerratings.com/store/#{_}") !!!)
          //remember jsoup doesn't support double quotes in attribute selector!
    .wgetInsertPagination("div#survey-header ul.pagination a:contains(next)")
    .save()
    .foreach(println(_))
  }
}
