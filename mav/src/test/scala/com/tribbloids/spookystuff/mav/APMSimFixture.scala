package com.tribbloids.spookystuff.mav

import com.tribbloids.spookystuff.mav.sim.APMSim
import com.tribbloids.spookystuff.session.{DriverSession, Session, TaskThreadInfo}
import com.tribbloids.spookystuff.{SpookyEnvFixture, caching}
import org.apache.spark.rdd.RDD

/**
  * Created by peng on 01/10/16.
  */
object SimFixture {

  val allSims: caching.ConcurrentSet[APMSim] = caching.ConcurrentSet()

  def launch(session: Session): APMSim = {
    val sim = APMSim.next
    sim.sessionPy(session)
    sim
  }
}

abstract class APMSimFixture extends SpookyEnvFixture {

  import com.tribbloids.spookystuff.utils.SpookyViews._

  override val pNames = Seq("phantomjs", "python", "apm")

  var simConnStrRDD: RDD[String] = _
  def simConnStrs = simConnStrRDD.collect().distinct

  override def beforeAll(): Unit = {
    super.beforeAll()
    val spooky = this.spooky
    val connStrRDD = sc.mapPerExecutor {
      //NOT cleaned by TaskCompletionListener
      val session = new DriverSession(spooky, TaskThreadInfo.thread)
      val sim = SimFixture.launch(session)
      SimFixture.allSims += sim
      sim.sessionPy(session).connStr.toStringOpt
    }
      .flatMap(v => v)
      .persist()

    connStrRDD.count()
    this.simConnStrRDD = connStrRDD
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

    println(s"connStrs:\n${this.simConnStrs.mkString("\n")}")
    assert(simConnStrs.nonEmpty)
    assert(simConnStrs.size == simConnStrs.distinct.size)
    assert(simConnStrs.size == iNums.size)
  }
}