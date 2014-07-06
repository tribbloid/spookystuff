package org.tribbloid.spookystuff.example

import org.tribbloid.spookystuff.SpookyContext._
import org.tribbloid.spookystuff.entity._
import org.apache.spark.SparkContext._

/**
 * This job will find and printout urls of Sanjay Gupta, Arun Gupta and Hardik Gupta in your area
 */
object WellCa extends SparkSubmittable {

  def doMain() {

    (sc.parallelize(Seq("Dummy")) +>
      Wget("http://well.ca/whatsnew/") !!!).saveAs(
        dir = "file:///home/peng/wellca", overwrite = true
      ).joinBySlice(
        "div.product_grid_full_categories"
      ).map{
      page =>
        (page.savePath,
          page.text("div.product_grid_info_top_text_container"))
    }.flatMapValues(texts => texts).map(
        tuple => tuple._1+"\t"+tuple._2
      ).saveAsTextFile("file:///home/peng/wellca/result")
  }
}
