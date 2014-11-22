package org.tribbloid.spookystuff.example.price

import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.expressions._
import org.tribbloid.spookystuff.example.ExampleCore

object Iherb extends ExampleCore {

  override def doMain(spooky: SpookyContext) = {
    import spooky._

    noInput
      .fetch(
        Wget("http://ca.iherb.com/")
      )
      .wgetJoin('* href "div.category a")()
      .wgetExplore('* href "p.pagination a:contains(Next)")(depthKey = 'page)
      .sliceJoin("div.prodSlotWide")(indexKey = 'row)
      .extract(
        "description" -> (_.text1("p.description")),
        "price" -> (_.text1("div.price")),
        "saved" -> (_.saved)
      )
      .asSchemaRDD()
  }
}