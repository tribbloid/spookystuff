package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.dsl.DriverFactories

class TestTrace_PoolingPhantomJS extends TestTrace_PhantomJS {

  override lazy val driverFactory = DriverFactories.PhantomJS().taskLocal
}