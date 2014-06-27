package org.tribbloid.spookystuff.example

import org.apache.spark.{SparkContext, SparkConf}
import org.tribbloid.spookystuff.{entity, Conf}
import org.tribbloid.spookystuff.entity._
import org.tribbloid.spookystuff.SpookyContext._

/**
 * Created by peng on 18/06/14.
 */
object ResellerRatingsUseContainer extends SparkSubmittable {

  def doMain() {

    (sc.parallelize(Seq("Hewlett_Packard"))
      +> Visit("http://www.resellerratings.com/store/#{_}")
      +> Loop()(
      Submit("div#survey-header ul.pagination a:contains(next)"),
      Snapshot()
    ) !!!)
      //remember jsoup doesn't support double quotes in attribute selector!
      .save()
      .foreach(println(_))
  }
}
