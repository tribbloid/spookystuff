package org.tribbloid.spookystuff.integration.price

import org.tribbloid.spookystuff.entity.client._
import org.tribbloid.spookystuff.integration.TestCore

object Iherb extends TestCore {

  import spooky._

  override def doMain() = {

    (noInput
      +> Wget("http://ca.iherb.com/")
      !=!())
      .wgetJoin("div.category a")()
      .paginate("p.pagination a:contains(Next)")(indexKey = "page", limit = 2)
      .sliceJoin("div.prodSlotWide")(indexKey = "row")
      .extract(
        "description" -> (_.text1("p.description")),
        "price" -> (_.text1("div.price")),
        "saved" -> (_.saved)
      )
      .asSchemaRDD()
  }
}