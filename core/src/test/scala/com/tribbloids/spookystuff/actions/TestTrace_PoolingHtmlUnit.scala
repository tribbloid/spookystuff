package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.dsl.DriverFactories

class TestTrace_PoolingHtmlUnit extends TestTrace_PhantomJS {

  override lazy val driverFactory = DriverFactories.HtmlUnit().taskLocal
}