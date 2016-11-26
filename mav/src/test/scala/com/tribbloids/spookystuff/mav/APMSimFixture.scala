package com.tribbloids.spookystuff.mav

import com.tribbloids.spookystuff.mav.sim.APMSim
import com.tribbloids.spookystuff.mav.telemetry.Link
import com.tribbloids.spookystuff.session.{AutoCleanable, DriverSession, Lifespan, Session}
import com.tribbloids.spookystuff.{SpookyEnvFixture, caching}
import org.apache.spark.rdd.RDD

/**
  * Created by peng on 01/10/16.
  */
object SimFixture {

  val allSims: caching.ConcurrentSet[APMSim] = caching.ConcurrentSet()

  def launch(session: Session): APMSim = {
    val sim = APMSim.next
    sim.Py(session)
    sim
  }
}

abstract class APMSimFixture extends SpookyEnvFixture {

  import com.tribbloids.spookystuff.utils.SpookyViews._

  override val pNames = Seq("phantomjs", "python", "apm")

  var simConnStrRDD: RDD[String] = _
  def simConnStrs = simConnStrRDD.collect().toSeq.distinct

  def parallelism: Int = sc.defaultParallelism

  override def beforeAll(): Unit = {
    super.beforeAll()
    val spooky = this.spooky

    val connStrRDD = sc.parallelize(1 to parallelism)
      .map {
        i =>
          //NOT cleaned by TaskCompletionListener
          val session = new DriverSession(spooky, Lifespan.JVM())
          val sim = SimFixture.launch(session)
          SimFixture.allSims += sim
          sim.Py(session).connStr.strOpt
      }
      .flatMap(v => v)
      .persist()

    connStrRDD.count()
    this.simConnStrRDD = connStrRDD
  }

  override def afterAll(): Unit = {
    sc.foreachNode {
      // in production Links will be cleaned up by shutdown hook, unfortunately test fixtures can't wait for that long.
      AutoCleanable.cleanupAll {
        case v: Link => true
        case _ => false
      }
      //TODO: run on each worker!

      val sims: Set[APMSim] = SimFixture.allSims.toSet
      val bindings = sims
        .flatMap(v => v.bindings)

      SimFixture.allSims.foreach {
        sim =>
          sim.tryClean()
      }
      bindings.foreach(_.driver.tryClean())
      SimFixture.allSims.clear()
    }
    super.afterAll()
  }
}

// TODO: implement! 1 SITL on drivers
class APMSimSingletonSuite extends SpookyEnvFixture {
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