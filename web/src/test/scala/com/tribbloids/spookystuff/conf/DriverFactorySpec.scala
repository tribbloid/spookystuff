package com.tribbloids.spookystuff.conf

import com.tribbloids.spookystuff.actions.Visit
import com.tribbloids.spookystuff.conf.{PluginSystem, Python, Web, WebDriverFactory}
import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.testutils.LocalPathDocsFixture
import com.tribbloids.spookystuff.{SpookyContext, SpookyEnvFixture}

class DriverFactorySpec extends SpookyEnvFixture with LocalPathDocsFixture {

  trait BaseCase {

    val pluginSys: PluginSystem.WithDrivers
    type Driver = pluginSys.Driver

    def driverFactory: DriverFactory.Transient[Driver]

    it(s"$driverFactory can factoryReset") {
      val session = new Session(spooky)
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

  PythonDriverCase
  WebDriverCase

  it("If the old driver is released, the second Pooling DriverFactory.get() should yield the same driver") {
    val conf = Web.Conf(WebDriverFactory.PhantomJS().taskLocal)

    val spooky = new SpookyContext(sql)
    spooky.setConf(conf)

    val session1 = new Session(spooky)
    Visit(HTML_URL).apply(session1)
    val driver1 = session1.driverOf(Web)
    session1.tryClean()

    val session2 = new Session(spooky)
    Visit(HTML_URL).apply(session2)
    val driver2 = session2.driverOf(Web)
    session2.tryClean()

    assert(driver1 eq driver2)
    driver1.tryClean()
  }
}
