package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.dsl.{DriverFactory, WebDriverFactory}

class TestTrace_PoolingHtmlUnit extends TestTrace_PhantomJS {

  override lazy val driverFactory = WebDriverFactory.HtmlUnit().taskLocal
}
