package org.tribbloid.spookystuff.actions

import org.tribbloid.spookystuff.SpookyEnvSuite
import org.tribbloid.spookystuff.dsl.HtmlUnitDriverFactory
import org.tribbloid.spookystuff.pages.Page

/**
 * Created by peng on 05/06/14.
 */

//TODO: this need some serious reorganization
class TestTraceWithHtmlUnit extends TestTrace {

  override val driverFactory = HtmlUnitDriverFactory()
}
