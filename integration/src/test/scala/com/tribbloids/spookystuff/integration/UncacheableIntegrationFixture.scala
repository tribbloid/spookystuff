package com.tribbloids.spookystuff.integration

abstract class UncacheableIntegrationFixture extends IntegrationFixture {

  override protected def doTest(): Unit = {

    doTestBeforeCache()
  }
}
