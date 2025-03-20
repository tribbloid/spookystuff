package com.tribbloids.spookystuff.web.actions

import com.tribbloids.spookystuff.web.conf.WebDriverFactory

class TestTrace_PhantomJS extends WebActionIT {

  override lazy val driverFactory: WebDriverFactory.PhantomJS = WebDriverFactory.PhantomJS()
}
