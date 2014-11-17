package org.tribbloid.spookystuff.example.price

import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.example.ExampleCore

/**
 * This job will find and printout urls of Sanjay Gupta, Arun Gupta and Hardik Gupta in your area
 */
object WellCa extends ExampleCore {

  override def doMain(spooky: SpookyContext) = {
    import spooky._

    noInput
      .fetch(
        Wget("http://well.ca/whatsnew/")
      )
      .sliceJoin("div.product_grid_full_categories")()
      .extract(
        "name" -> (_.text1("div.product_grid_info_top_text_container"))
      )
      .asSchemaRDD()
  }
}
