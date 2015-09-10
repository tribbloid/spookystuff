package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.dsl.DriverFactories

/**
 * Created by peng on 05/06/14.
 */

//TODO: this need some serious reorganization
class TestTraceWithHtmlUnit extends TestTrace {

  override lazy val driverFactory = DriverFactories.HtmlUnit()
}
