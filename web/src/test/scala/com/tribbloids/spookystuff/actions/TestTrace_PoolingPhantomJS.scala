package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.conf.WebDriverFactory

class TestTrace_PoolingPhantomJS extends AbstractTestTrace {

  override lazy val driverFactory = WebDriverFactory.PhantomJS().taskLocal
}
