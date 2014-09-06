package org.tribbloid.spookystuff.integration

import org.tribbloid.spookystuff.entity._

/**
 * This job will find and printout urls of Sanjay Gupta, Arun Gupta and Hardik Gupta in your area
 */
object WellCa extends SpookyTestCore {

  import spooky._

  def doMain() = {

    (sc.parallelize(Seq(null))
      +> Visit("http://well.ca/whatsnew/")
      !=!())
      .saveAs(dir = "file:///home/peng/spookystuff/wellca")
      .sliceJoin("div.product_grid_full_categories")()
      .select(
        "name" -> (_.text1("div.product_grid_info_top_text_container"))
      )
      .asSchemaRDD()
  }
}
