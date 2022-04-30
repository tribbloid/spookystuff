package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.conf.{DriverFactory, WebDriverFactory}

class TestTrace_PoolingHtmlUnit extends AbstractTestTrace {

  override lazy val driverFactory = WebDriverFactory.HtmlUnit().taskLocal
}
