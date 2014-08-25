package org.tribbloid.spookystuff.acceptance.cloud

import org.tribbloid.spookystuff.SpookyContext._
import org.tribbloid.spookystuff.acceptance.SparkTestCore
import org.tribbloid.spookystuff.entity._

/**
 * Created by peng on 25/08/14.
 */
object DigitalOcean extends SparkTestCore {

  override def doMain(): Array[_] = {
    (sc.parallelize(Seq(null)) +>
      Visit("https://www.digitalocean.com/pricing/") !==)
      .leftJoinBySlice(
        "div.plan"
      ).selectInto(
        "Memory" -> {_.text("ul li")(0)},
        "Core" -> {_.text("ul li")(1)},
        "Drive" -> {_.text("ul li")(2)},
        "Transfer" -> {_.text("ul li")(3)},
        "price_monthly" -> {_.attr1("span.amount","data-dollar-amount")},
        "price_hourly" -> {_.attr1("span.amount","data-hourly-amount")}
      ).asJsonRDD
      .collect()
  }
}
