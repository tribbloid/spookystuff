package com.tribbloids.spookystuff.dsl

import com.tribbloids.spookystuff.actions.Visit
import com.tribbloids.spookystuff.session.DriverSession
import com.tribbloids.spookystuff.testutils.LocalPathDocsFixture
import com.tribbloids.spookystuff.{SpookyConf, SpookyContext, SpookyEnvFixture}

/**
  * Created by peng on 22/10/16.
  */
class TestDriverFactory extends SpookyEnvFixture with LocalPathDocsFixture {

  val baseFactories: Seq[TransientFactory[_]] = Seq(
    DriverFactories.PhantomJS(),
    DriverFactories.Python()
  )

  val poolingFactories: Seq[TaskLocalFactory[_]] = baseFactories
    .map(
      _.pooling
    )

  val factories = baseFactories ++ poolingFactories

  for (ff <- baseFactories) {
    test(s"$ff can factoryReset()") {
      val session = new DriverSession(spooky)
      val driver = ff.provision(session)
      ff.asInstanceOf[TransientFactory[Any]].factoryReset(driver.asInstanceOf[Any])
      ff.asInstanceOf[TransientFactory[Any]].destroy(driver, session.taskOpt)
    }
  }

//  for (ff <- poolingFactories) {
//
//  }
  test("If the old driver is released, the second Pooling DriverFactory.get() should yield the same driver") {
    val conf = new SpookyConf(webDriverFactory = DriverFactories.PhantomJS().pooling)
    val spooky = new SpookyContext(sql, conf)

    val session1 = new DriverSession(spooky)
    Visit(HTML_URL).apply(session1)
    val driver1 = session1.webDriver
    session1.clean()

    val session2 = new DriverSession(spooky)
    Visit(HTML_URL).apply(session2)
    val driver2 = session2.webDriver
    session2.clean()

    assert(driver1 eq driver2)
    driver1.clean()
  }
}