package org.tribbloid.spookystuff.integration

import org.tribbloid.spookystuff.entity.client._

/**
 * This job will find and printout urls of Sanjay Gupta, Arun Gupta and Hardik Gupta in your area
 */
object WellCa extends SpookyTestCore {

  import spooky._

  override def doMain() = {

    (noInput
      +> Visit("http://well.ca/whatsnew/")
      !=!())
      .sliceJoin("div.product_grid_full_categories")()
      .extract(
        "name" -> (_.text1("div.product_grid_info_top_text_container"))
      )
      .asSchemaRDD()
  }
}
