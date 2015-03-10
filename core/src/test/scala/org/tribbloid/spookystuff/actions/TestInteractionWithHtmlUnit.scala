package org.tribbloid.spookystuff.actions

import org.tribbloid.spookystuff.SpookyEnvSuite
import org.tribbloid.spookystuff.dsl.HtmlUnitDriverFactory
import org.tribbloid.spookystuff.pages.Page
import org.tribbloid.spookystuff.session.DriverSession

import scala.concurrent.duration._

/**
 * Created by peng on 2/19/15.
 */
class TestInteractionWithHtmlUnit extends TestInteraction {

  override val driverFactory = HtmlUnitDriverFactory()
}