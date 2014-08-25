package org.tribbloid.spookystuff.acceptance.cloud

import org.tribbloid.spookystuff.SpookyContext._
import org.tribbloid.spookystuff.acceptance.SparkTestCore
import org.tribbloid.spookystuff.entity._

/**
 * Created by peng on 25/08/14.
 */
object Singlehop extends SparkTestCore {

  override def doMain(): Array[_] = {
    (sc.parallelize(Seq(null))+>
      Wget("http://www.singlehop.com/server-hosting/dedicated-servers/") !==)
      .leftJoinBySlice(
        "tbody tr"
      ).selectInto(
        "CPU" -> {_.text1("span")},
        "Core/Freq" -> {_.text("td")(2)},
        "Memory" -> {_.text("td")(3)},
        "Drive" -> {_.text("td")(4)},
        "Bandwidth" -> {_.text("td")(5)},
        "Availability" -> {_.text("td")(6)},
        "price" -> {_.text1("b")}
      ).asJsonRDD
      .collect()
  }
}
