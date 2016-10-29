package com.tribbloids.spookystuff.mav

import com.tribbloids.spookystuff.mav.sim.APMSim
import com.tribbloids.spookystuff.session.{DriverSession, Session}
import com.tribbloids.spookystuff.{SpookyEnvFixture, caching}

/**
  * Created by peng on 01/10/16.
  */
object SimFixture {

  val allSims: caching.ConcurrentSet[APMSim] = caching.ConcurrentSet()

  def launch(session: Session): APMSim = {
    val sim = APMSim.next
    val py = sim.Py(session)
    py.varName
    allSims += sim
    sim
  }
}

abstract class APMSimFixture extends SpookyEnvFixture {

  import com.tribbloids.spookystuff.utils.SpookyViews._

  override def beforeAll(): Unit = {
    super.beforeAll()
    val spooky = this.spooky
    sc.foreachExecutor {
      val session = new DriverSession(spooky)
      SimFixture.launch(session)
    }
  }

  override def afterAll(): Unit = {
    sc.foreachNode {
      SimFixture.allSims.foreach {
        sim =>
          sim.finalize()
      }
      SimFixture.allSims.clear()
    }
    super.afterAll()
  }
}

class APMSimSuite extends APMSimFixture {

  test("should create many APM instances") {

  }


}