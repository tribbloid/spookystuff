package org.tribbloid.spookystuff.example.price.cloud

import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.example.ExampleCore

/**
* Created by peng on 25/08/14.
*/
object DigitalOcean extends ExampleCore {

  override def doMain(spooky: SpookyContext) = {
    import spooky._
    noInput
      .fetch(
        Visit("https://www.digitalocean.com/pricing/")
      )
      .sliceJoin("div.plan")(indexKey = 'row)
      .extract(
        "Memory" -> {_.text("ul li")(0)},
        "Core" -> {_.text("ul li")(1)},
        "Drive" -> {_.text("ul li")(2)},
        "Transfer" -> {_.text("ul li")(3)},
        "price_monthly" -> {_.attr1("span.amount","data-dollar-amount")},
        "price_hourly" -> {_.attr1("span.amount","data-hourly-amount")}
      )
      .asSchemaRDD()
  }
}
