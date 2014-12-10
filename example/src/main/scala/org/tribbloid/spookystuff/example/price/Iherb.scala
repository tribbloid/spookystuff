package org.tribbloid.spookystuff.example.price

import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.dsl._
import org.tribbloid.spookystuff.example.ExampleCore

object Iherb extends ExampleCore {

  override def doMain(spooky: SpookyContext) = {
    import spooky._

    noInput
      .fetch(
        Wget("http://ca.iherb.com/")
      )
      .wgetJoin($"div.category a")
      .wgetExplore($"p.pagination a:contains(Next)", depthKey = 'page)
      .flatSelect($"div.prodSlotWide", indexKey = 'row)(
        A"p.description".text > 'description,
        A"div.price".text > 'price,
        $.saved > 'saved,
        $.uri > 'uri
      )
      .asSchemaRDD()
  }
}