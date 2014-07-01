package org.tribbloid.spookystuff.example.largescale

import org.apache.spark.SparkContext._
import org.tribbloid.spookystuff.SpookyContext._
import org.tribbloid.spookystuff.entity._
import org.tribbloid.spookystuff.example.SparkSubmittable

object Iherb extends SparkSubmittable {

  def doMain() {

    (sc.parallelize(Seq("Dummy")) +>
      Wget("http://ca.iherb.com/")!!!).wgetJoin(
        "div.category a"
      ).wgetInsertPagination(
        "p.pagination a:contains(Next)", 1000
      ).saveAs(
        dir = "s3n://iherb", overwrite = true
      ).map{
      page =>
        (page.savePath,
          page.text("p.description"))
    }.flatMapValues(texts => texts).map(
        tuple => tuple._1+"\t"+tuple._2
      ).saveAsTextFile("s3n://iherb/result")
  }
}
