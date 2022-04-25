package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.conf.WebDriverFactory

class TestTrace_PhantomJS extends AbstractTestTrace {

  override lazy val driverFactory: WebDriverFactory.PhantomJS = WebDriverFactory.PhantomJS()
}
