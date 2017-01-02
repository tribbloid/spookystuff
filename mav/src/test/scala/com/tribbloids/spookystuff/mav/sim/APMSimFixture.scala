package com.tribbloids.spookystuff.mav.sim

import com.tribbloids.spookystuff.SpookyEnvFixture
import com.tribbloids.spookystuff.mav.MAVConf
import com.tribbloids.spookystuff.mav.telemetry.{Endpoint, Link}
import com.tribbloids.spookystuff.session.python.PythonDriver
import com.tribbloids.spookystuff.session.{Cleanable, Lifespan}
import org.apache.spark.rdd.RDD
import org.jutils.jprocesses.JProcesses
import org.slf4j.LoggerFactory

/**
  * Created by peng on 01/10/16.
  */
//object SimFixture {
//
//  def launch(session: AbstractSession): APMSim = {
//    val sim = APMSim.next
//    sim.Py(session)
//    sim
//  }
//}

abstract class APMSimFixture extends SpookyEnvFixture {

  import com.tribbloids.spookystuff.utils.SpookyViews._

  override val pNames = Seq("phantomjs", "python", "apm")

  var simConnStrRDD: RDD[String] = _
  def simConnStrs = simConnStrRDD.collect().toSeq.distinct
  def simEndpoints = simConnStrs.map(v => Endpoint(Seq(v)))

  def parallelism: Int = sc.defaultParallelism
  //  def parallelism: Int = 3

  override def setUp(): Unit = {
    super.setUp()
    val mavConf = this.spooky.conf.submodules.get[MAVConf]()
    mavConf.connectionRetries = 1 // do not retry
    mavConf.endpoints = simEndpoints
  }

  override def beforeAll(): Unit = {
    cleanSweep()

    super.beforeAll()
    val spooky = this.spooky

    val isEmpty = sc.mapPerComputer {APMSim.existing.isEmpty}.collect()
    assert(!isEmpty.contains(false))

    val connStrRDD = sc.parallelize(1 to parallelism)
      .map {
        i =>
          //NOT cleaned by TaskCompletionListener
          val apmSimDriver = new PythonDriver(lifespan = Lifespan.JVM(nameOpt = Some(s"APMSim-$i")))
          val sim = APMSim.next()
          sim._Py(apmSimDriver).connStr.$STR
      }
      .flatMap(v => v)
      .persist()

    val info = connStrRDD.collect().mkString("\n")
    LoggerFactory.getLogger(this.getClass).info(
      s"""
         |APM simulation(s) are up and running:
         |$info
      """.stripMargin
    )
    this.simConnStrRDD = connStrRDD
  }

  override def afterAll(): Unit = {
    cleanSweep()
    super.afterAll()
  }

  private def cleanSweep() = {
    sc.foreachComputer {
      // in production Links will be cleaned up by shutdown hook, unfortunately test suites can't wait for that long.

      Cleanable.cleanSweepAll {
        case v: Link => true // required as midair Links are only idled but never cleaned.
        case v: APMSim => true
        case _ => false
      }
    }
  }
}

class APMSimSuite extends APMSimFixture {

  import com.tribbloids.spookystuff.utils.SpookyViews._

  /**
    * this test assumes that all test runs on a single machine, so all SITL instance number has to be different
    */
  test("should create many APM instances with different iNum") {
    val iNums = sc.mapPerWorker {
      APMSim.existing.map(_.iNum)
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

    import scala.collection.JavaConverters._

    val processes = JProcesses.getProcessList()
      .asScala

    //ensure all apm processes are running
    val apmPs = processes.filter(_.getName == "apm")
    assert(apmPs.size == parallelism)
  }
}
