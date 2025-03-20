package com.tribbloids.spookystuff.web.actions

import com.tribbloids.spookystuff.web.conf.WebDriverFactory

class TestTrace_HtmlUnit extends WebActionIT {

  override lazy val driverFactory: WebDriverFactory.HtmlUnit = WebDriverFactory.HtmlUnit()

}
