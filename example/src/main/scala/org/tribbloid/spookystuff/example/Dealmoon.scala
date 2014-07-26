package org.tribbloid.spookystuff.example

import org.tribbloid.spookystuff.SpookyContext._
import org.tribbloid.spookystuff.entity._
import org.apache.spark.SparkContext._

object Dealmoon extends SparkSubmittable {

  def doMain() {

    (sc.parallelize(Seq("Dummy")) +>
      Wget("http://www.dealmoon.com/Online-Stores/Amazon-com?expired=n")!).wgetInsertPagination(
        "a.next_link"
      ).saveAs(
        dir = "s3n://dealmoon", overwrite = true
      ).map{
      page =>
        (page.savePath,
          page.text("div.mlist div.mtxt h2 span:not([style])"))
    }.flatMapValues(texts => texts).map(
        tuple => tuple._1+"\t"+tuple._2
      ).saveAsTextFile("s3n://dealmoon/result")
  }
}
