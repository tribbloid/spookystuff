package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.conf.WebDriverFactory
import com.tribbloids.spookystuff.dsl.DriverFactory

class TestTrace_PoolingHtmlUnit extends AbstractTestTrace {

  override lazy val driverFactory = WebDriverFactory.HtmlUnit().taskLocal
}
