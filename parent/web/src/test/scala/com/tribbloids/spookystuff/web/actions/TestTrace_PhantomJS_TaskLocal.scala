package com.tribbloids.spookystuff.web.actions

import com.tribbloids.spookystuff.web.conf.WebDriverFactory

class TestTrace_PhantomJS_TaskLocal extends AbstractWebActionSpec {

  override lazy val driverFactory = WebDriverFactory.PhantomJS().taskLocal
}
