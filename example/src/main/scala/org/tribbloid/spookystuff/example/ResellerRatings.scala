package org.tribbloid.spookystuff.example

import org.apache.spark.{SparkContext, SparkConf}
import org.tribbloid.spookystuff.Conf
import org.tribbloid.spookystuff.entity._
import org.tribbloid.spookystuff.SpookyContext._

/**
 * Created by peng on 18/06/14.
 */
object ResellerRatings {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("MoreLinkedIn")
    conf.setMaster("local[8,3]")
    //    conf.setMaster("local-cluster[2,4,1000]")
    conf.setSparkHome(System.getenv("SPARK_HOME"))
    val jars = SparkContext.jarOfClass(this.getClass).toList
    conf.setJars(jars)
    conf.set("spark.task.maxFailures", "3")
    val sc = new SparkContext(conf)

    Conf.init(sc)

    (sc.parallelize(Seq("Hewlett_Packard")) +>
      Wget("http://www.resellerratings.com/store/#{_}") !!!)
          //remember jsoup doesn't support double quotes in attribute selector!
    .wgetJoinByPagination("div#survey-header ul.pagination a:contains(next)")
    .save()
    .collect().foreach(println(_))

    sc.stop()
  }
}
