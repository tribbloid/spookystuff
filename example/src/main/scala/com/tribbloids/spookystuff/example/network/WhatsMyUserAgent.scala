package com.tribbloids.spookystuff.example.network

import com.tribbloids.spookystuff.{dsl, SpookyContext}
import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.example.QueryCore
import dsl._

/**
 * Created by peng on 9/7/14.
 */
object WhatsMyUserAgent extends QueryCore {

  override def doMain(spooky: SpookyContext) = {
    import spooky.dsl._

    //    spooky.driverFactory = TorDriverFactory()

    spooky
      .fetch(
        Visit("http://www.whatsmyuseragent.com/")
      )
      .select(
        S"h3.info".text ~ 'ip,
        S"h2.info".text ~ 'user_agent,
        S"table.table-striped".text ~ 'referer
      )
      .toDF()
  }
}