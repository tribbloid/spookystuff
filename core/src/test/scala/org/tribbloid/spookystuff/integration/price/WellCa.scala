package org.tribbloid.spookystuff.integration.price

import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.integration.TestCore

/**
 * This job will find and printout urls of Sanjay Gupta, Arun Gupta and Hardik Gupta in your area
 */
object WellCa extends TestCore {

  import spooky._

  override def doMain() = {

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
