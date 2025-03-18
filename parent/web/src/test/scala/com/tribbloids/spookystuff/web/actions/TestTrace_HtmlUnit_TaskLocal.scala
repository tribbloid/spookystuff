package com.tribbloids.spookystuff.web.actions

import com.tribbloids.spookystuff.web.conf.WebDriverFactory

class TestTrace_HtmlUnit_TaskLocal extends AbstractWebActionSpec {

  override lazy val driverFactory = WebDriverFactory.HtmlUnit().taskLocal
}
