package com.tribbloids.spookystuff.web.actions

import com.tribbloids.spookystuff.web.conf.WebDriverFactory

class TestTrace_PoolingHtmlUnit extends AbstractTestTrace {

  override lazy val driverFactory = WebDriverFactory.HtmlUnit().taskLocal
}
