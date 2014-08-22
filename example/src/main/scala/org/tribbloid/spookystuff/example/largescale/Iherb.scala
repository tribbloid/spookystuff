package org.tribbloid.spookystuff.example.largescale

import org.tribbloid.spookystuff.SpookyContext._
import org.tribbloid.spookystuff.entity._
import org.tribbloid.spookystuff.example.SparkSubmittable

object Iherb extends SparkSubmittable {

  def doMain() {

    (sc.parallelize(Seq("Dummy")) +>
      Wget("http://ca.iherb.com/")!==).wgetJoin(
        "div.category a"
      ).wgetInsertPagination(
        "p.pagination a:contains(Next)", 1000
      ).saveAs(
        dir = "s3n://spookystuff/iherb", overwrite = true
      ).joinBySlice(
        "div.prodSlotWide"
      ).map {
      page => (
        page.savePath,
        page.text1("p.description"),
        page.text1("div.price")
        ).productIterator.toList.mkString("\t")
    }.saveAsTextFile("s3n://spookystuff/iherb/result")
  }
}