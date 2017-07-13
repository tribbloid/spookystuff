package com.tribbloids.spookystuff.integration.join

import com.tribbloids.spookystuff.actions.Wget
import com.tribbloids.spookystuff.extractors.Col

/**
 * Created by peng on 25/10/15.
 */
class InnerWgetJoinIT extends InnerVisitJoinIT {

  override lazy val driverFactories = Seq(
    null
  )

  override def getPage(uri: Col[String]) = Wget(uri)
}
