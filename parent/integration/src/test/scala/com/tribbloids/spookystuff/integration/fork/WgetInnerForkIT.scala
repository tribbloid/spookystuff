package com.tribbloids.spookystuff.integration.fork

import com.tribbloids.spookystuff.actions.Wget
import com.tribbloids.spookystuff.extractors.Col

/**
  * Created by peng on 25/10/15.
  */
class WgetInnerForkIT extends VisitInnerForkIT {

  override lazy val webDriverFactories = Seq(
    null
  )

  override def getPage(uri: Col[String]) = Wget(uri)
}
