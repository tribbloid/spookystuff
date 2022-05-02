package com.tribbloids.spookystuff.web.actions

import com.tribbloids.spookystuff.web.conf.WebDriverFactory

class TestTrace_PhantomJS extends AbstractTestTrace {

  override lazy val driverFactory: WebDriverFactory.PhantomJS = WebDriverFactory.PhantomJS()
}
