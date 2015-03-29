package org.tribbloid.spookystuff.example.price

import org.tribbloid.spookystuff.{dsl, SpookyContext}
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.example.QueryCore
import dsl._

/**
 * This job will find and printout urls of Sanjay Gupta, Arun Gupta and Hardik Gupta in your area
 */
object WellCa extends QueryCore {

  override def doMain(spooky: SpookyContext) = {
    import spooky._

    noInput
      .fetch(
        Wget("http://well.ca/whatsnew/")
      )
      .flatSelect($"div.product_grid_full_categories")(
        A"div.product_grid_info_top_text_container".text ~ 'name
      )
      .toDataFrame()
  }
}
