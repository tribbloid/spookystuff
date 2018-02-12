package com.tribbloids.spookystuff.dsl

import com.tribbloids.spookystuff.actions.Visit
import com.tribbloids.spookystuff.conf.SpookyConf
import com.tribbloids.spookystuff.dsl.DriverFactories.{TaskLocal, Transient}
import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.testutils.LocalPathDocsFixture
import com.tribbloids.spookystuff.{SpookyContext, SpookyEnvFixture}

/**
  * Created by peng on 22/10/16.
  */
class TestDriverFactory extends SpookyEnvFixture with LocalPathDocsFixture {

  val baseFactories: Seq[Transient[_]] = Seq(
    DriverFactories.PhantomJS(),
    DriverFactories.Python(_ => "python3")
  )

  val poolingFactories: Seq[TaskLocal[_]] = baseFactories
    .map(
      _.taskLocal
    )

  val factories = baseFactories ++ poolingFactories

  for (ff <- baseFactories) {
    it(s"$ff can factoryReset()") {
      val session = new Session(spooky)
      val driver = ff.dispatch(session)
      ff.asInstanceOf[Transient[Any]].factoryReset(driver.asInstanceOf[Any])
      ff.asInstanceOf[Transient[Any]].destroy(driver, session.taskContextOpt)
    }
  }

  //  for (ff <- poolingFactories) {
  //
  //  }
  it("If the old driver is released, the second Pooling DriverFactory.get() should yield the same driver") {
    val conf = new SpookyConf(webDriverFactory = DriverFactories.PhantomJS().taskLocal)
    val spooky = new SpookyContext(sql, conf)

    val session1 = new Session(spooky)
    Visit(HTML_URL).apply(session1)
    val driver1 = session1.webDriver
    session1.tryClean()

    val session2 = new Session(spooky)
    Visit(HTML_URL).apply(session2)
    val driver2 = session2.webDriver
    session2.tryClean()

    assert(driver1 eq driver2)
    driver1.tryClean()
  }
}