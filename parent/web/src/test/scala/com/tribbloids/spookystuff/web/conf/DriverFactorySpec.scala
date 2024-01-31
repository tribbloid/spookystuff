package com.tribbloids.spookystuff.web.conf

import com.tribbloids.spookystuff.conf.DriverFactory.Transient
import com.tribbloids.spookystuff.conf.{DriverFactory, PluginSystem, Python, PythonDriverFactory}
import com.tribbloids.spookystuff.session.Agent
import com.tribbloids.spookystuff.testutils.{LocalPathDocsFixture, SpookyBaseSpec}
import com.tribbloids.spookystuff.web.actions.Visit
import com.tribbloids.spookystuff.SpookyContext

class DriverFactorySpec extends SpookyBaseSpec with LocalPathDocsFixture {

  trait BaseCase {

    val pluginSys: PluginSystem.HasDriver
    type Driver = pluginSys.Driver

    def driverFactory: Transient[Driver]

    lazy val taskLocalDriverFactory: DriverFactory.TaskLocal[Driver] = driverFactory.taskLocal

    it(s"$driverFactory can factoryReset") {
      val session = new Agent(spooky)
      val driver = driverFactory.dispatch(session)
      driverFactory.factoryReset(driver)
      driverFactory.destroy(driver, session.taskContextOpt)
    }

//    object TaskLocalCase {
//
//      def _outer: BaseCase.this.type = BaseCase.this
//
//      lazy val taskLocalDriverF: DriverFactory.TaskLocal[Driver] = _outer.driverFactory.taskLocal
//
//    }
  }

  object PythonDriverCase extends BaseCase {
    override val pluginSys: Python.type = Python
    override lazy val driverFactory = PythonDriverFactory._3
  }

  object WebDriverCase extends BaseCase {
    override val pluginSys: Web.type = Web
    override lazy val driverFactory = WebDriverFactory.PhantomJS()
  }

  object HtmlUnitDriverCase extends BaseCase {
    override val pluginSys: Web.type = Web
    override lazy val driverFactory = WebDriverFactory.HtmlUnit()
  }

  PythonDriverCase
  WebDriverCase
  HtmlUnitDriverCase

  it("If the old driver is released, the second taskLocal DriverFactory.get() should yield the same driver") {
    val conf = Web.Conf(WebDriverFactory.PhantomJS().taskLocal)

    val spooky = new SpookyContext(sql)
    spooky.setConf(conf)

    val session1 = new Agent(spooky)
    Visit(HTML_URL).apply(session1)
    val driver1 = session1.driverOf(Web)
    session1.tryClean()

    val session2 = new Agent(spooky)
    Visit(HTML_URL).apply(session2)
    val driver2 = session2.driverOf(Web)
    session2.tryClean()

    assert(driver1 eq driver2)
    driver1.tryClean()
  }
}
