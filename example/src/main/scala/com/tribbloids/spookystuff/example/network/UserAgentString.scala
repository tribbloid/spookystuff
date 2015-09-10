package com.tribbloids.spookystuff.example.network

import com.tribbloids.spookystuff.{dsl, SpookyContext}
import com.tribbloids.spookystuff.actions._
import com.tribbloids.spookystuff.example.QueryCore
import dsl._

/**
 * Created by peng on 9/7/14.
 */
object UserAgentString extends QueryCore {

  override def doMain(spooky: SpookyContext) = {
    import spooky.dsl._

    //    spooky.driverFactory = TorDriverFactory()

    spooky
      .fetch(
        Wget("http://www.useragentstring.com/pages/Browserlist/")
      )
      .select(S"li a".text ~ 'agent_string)
      .toDF()
  }
}