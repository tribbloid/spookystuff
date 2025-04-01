package com.tribbloids.spookystuff.integration

abstract class UncacheableIntegrationFixture extends ITBaseSpec {

  override protected def doTest(): Unit = {

    doTestBeforeCache()
  }
}
