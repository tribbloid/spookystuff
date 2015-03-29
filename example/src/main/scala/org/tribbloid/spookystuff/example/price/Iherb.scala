package org.tribbloid.spookystuff.example.price

import org.tribbloid.spookystuff.SpookyContext
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.dsl._
import org.tribbloid.spookystuff.example.QueryCore

object Iherb extends QueryCore {

  override def doMain(spooky: SpookyContext) = {
    import spooky._

    spooky
      .fetch(
        Wget("http://ca.iherb.com/")
      )
      .wgetJoin($"div.category a", ordinalKey = 'category_index)
      .wgetExplore($"p.pagination a", select = 'A.text ~ 'page, optimizer = Narrow)
      .flatSelect($"div.prodSlotWide", ordinalKey = 'row)(
        A"p.description".text ~ 'description,
        A"div.price".text ~ 'price,
        $.saved ~ 'saved,
        $.uri ~ 'uri
      )
      .toDataFrame()
  }
}