package com.tribbloids.spookystuff.web.conf

import ai.acyclic.prover.commons.spark.TestHelper
import ai.acyclic.prover.commons.spark.serialization.AssertSerializable
import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.agent.Agent
import com.tribbloids.spookystuff.conf.DriverFactory
import com.tribbloids.spookystuff.testutils.{FileURIDocsFixture, SpookyBaseSpec}
import com.tribbloids.spookystuff.web.actions.Visit
import com.tribbloids.spookystuff.web.agent.CleanWebDriver

trait WebDriverCase extends SpookyBaseSpec with FileURIDocsFixture {

  def transientDriverFactory: DriverFactory.Transient[CleanWebDriver]

  it("is serializable") {

    AssertSerializable(transientDriverFactory).weakly()
    AssertSerializable(transientDriverFactory.taskLocal).weakly()
  }

  it("factoryReset") {
    val agent = new Agent(spooky)
    val driver = transientDriverFactory.dispatch(agent)

//    driver.get("https://www.selenium.dev/documentation/selenium_manager/")
    driver.get(HTML_URL)

    transientDriverFactory.factoryReset(driver)
    assert(driver.getTitle == "")

    driver.clean(false)
  }

  it("If a taskLocal driver is released, it can be reused") {
    val conf = Web.Conf(transientDriverFactory.taskLocal)

    val spooky = new SpookyContext(TestHelper.TestSparkSession)
    spooky.setConf(conf)

    val a1 = new Agent(spooky)
    Visit(HTML_URL).apply(a1)
    val driver1 = a1.getDriver(Web)
    a1.tryClean()

    val a2 = new Agent(spooky)
    Visit(HTML_URL).apply(a2)
    val driver2 = a2.getDriver(Web)
    a2.tryClean()

    assert(driver1 eq driver2)
    driver1.tryClean()
  }
}
