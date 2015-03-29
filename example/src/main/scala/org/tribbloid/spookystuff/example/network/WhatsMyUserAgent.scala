package org.tribbloid.spookystuff.example.network

import org.tribbloid.spookystuff.{dsl, SpookyContext}
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.example.QueryCore
import dsl._

/**
 * Created by peng on 9/7/14.
 */
object WhatsMyUserAgent extends QueryCore {

  override def doMain(spooky: SpookyContext) = {
    import spooky._

    //    spooky.driverFactory = TorDriverFactory()

    spooky
      .fetch(
        Visit("http://www.whatsmyuseragent.com/")
      )
      .select(
        $"h3.info".text ~ 'ip,
        $"h2.info".text ~ 'user_agent,
        $"table.table-striped".text ~ 'referer
      )
      .toDataFrame()
  }
}