package org.tribbloid.spookystuff.example.price

import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.expressions._
import org.tribbloid.spookystuff.example.TestCore

object Iherb extends TestCore {

  import spooky._

  override def doMain() = {

    noInput
      .fetch(
        Wget("http://ca.iherb.com/")
      )
      .wgetJoin('* href "div.category a")()
      .paginate("p.pagination a:contains(Next)")(indexKey = 'page, limit = 2)
      .sliceJoin("div.prodSlotWide")(indexKey = 'row)
      .extract(
        "description" -> (_.text1("p.description")),
        "price" -> (_.text1("div.price")),
        "saved" -> (_.saved)
      )
      .asSchemaRDD()
  }
}