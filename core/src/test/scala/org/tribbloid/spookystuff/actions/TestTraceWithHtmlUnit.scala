package org.tribbloid.spookystuff.actions

import org.tribbloid.spookystuff.dsl.DriverFactories

/**
 * Created by peng on 05/06/14.
 */

//TODO: this need some serious reorganization
class TestTraceWithHtmlUnit extends TestTrace {

  override lazy val driverFactory = DriverFactories.HtmlUnit()
}
