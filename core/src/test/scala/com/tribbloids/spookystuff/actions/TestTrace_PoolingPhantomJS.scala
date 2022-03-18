package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.dsl.WebDriverFactory

class TestTrace_PoolingPhantomJS extends TestTrace_PhantomJS {

  override lazy val driverFactory = WebDriverFactory.PhantomJS().taskLocal
}
