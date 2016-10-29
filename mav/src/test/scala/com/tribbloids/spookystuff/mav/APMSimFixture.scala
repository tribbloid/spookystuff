package com.tribbloids.spookystuff.mav

import com.tribbloids.spookystuff.mav.sim.APMSim
import com.tribbloids.spookystuff.session.{DriverSession, Session, TaskThreadInfo}
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
    sim
  }
}

abstract class APMSimFixture extends SpookyEnvFixture {

  import com.tribbloids.spookystuff.utils.SpookyViews._

  override val pNames = Seq("phantomjs", "python", "apm")

  override def beforeAll(): Unit = {
    super.beforeAll()
    val spooky = this.spooky
    sc.foreachExecutor {
      //NOT cleaned by TaskCompletionListener
      val session = new DriverSession(spooky, TaskThreadInfo.thread)
      val sim = SimFixture.launch(session)
      SimFixture.allSims += sim
    }
  }

  override def afterAll(): Unit = {
    sc.foreachNode {
      val sims: Set[APMSim] = SimFixture.allSims.toSet
      val bindings = sims
        .flatMap(v => v.bindings)

      SimFixture.allSims.foreach {
        sim =>
          sim.finalize()
      }
      bindings.foreach(_.driver.finalize())
      SimFixture.allSims.clear()
    }
    super.afterAll()
  }
}

class APMSimSuite extends APMSimFixture {

  import com.tribbloids.spookystuff.utils.SpookyViews._

  /**
    * this test assumes that all test runs on a single machine, so all SITL instance number has to be different
    */
  test("should create many APM instances with different iNum") {
    val iNums = sc.mapPerNode {
      SimFixture.allSims.map(_.iNum)
    }
      .collect()
      .toSeq
      .flatMap(_.toSeq)

    println(s"iNums: ${iNums.mkString(", ")}")
    assert(iNums.nonEmpty)
    assert(iNums.size == iNums.distinct.size)
  }
}