package com.tribbloids.spookystuff.integration.join

import com.tribbloids.spookystuff.actions.Wget
import com.tribbloids.spookystuff.expressions.Expression

/**
 * Created by peng on 25/10/15.
 */
class LeftWgetJoinIT extends LeftVisitJoinIT {

  override lazy val drivers = Seq(
    null
  )

  override def getPage(uri: Expression[String]) = Wget(uri)

  override def numDrivers = 0
}
