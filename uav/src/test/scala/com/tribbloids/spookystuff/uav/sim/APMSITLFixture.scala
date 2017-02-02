package com.tribbloids.spookystuff.uav.sim

import com.tribbloids.spookystuff.SpookyEnvFixture
import com.tribbloids.spookystuff.uav.MAVConf
import com.tribbloids.spookystuff.uav.system.Drone
import com.tribbloids.spookystuff.uav.telemetry.Link
import com.tribbloids.spookystuff.session.python.PythonDriver
import com.tribbloids.spookystuff.session.{Cleanable, Lifespan}
import com.tribbloids.spookystuff.utils.SpookyUtils
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

trait SIMFixture extends SpookyEnvFixture {

  var simURIRDD: RDD[String] = _
  def simURIs = simURIRDD.collect().toSeq.distinct
  def simDrones = simURIs.map(v => Drone(Seq(v)))

  def parallelism: Int = sc.defaultParallelism
  //  def parallelism: Int = 3
}

trait APMSITLFixture extends SIMFixture {

  val getNext: () => APMSim = {
    () =>
      APMSim.next(vType = "copter", version = "3.3")
  }

  import com.tribbloids.spookystuff.uav.dsl._
  import com.tribbloids.spookystuff.utils.SpookyViews._

  override val processNames = Seq("phantomjs", "python", "apm")

  override def setUp(): Unit = {
    super.setUp()
    val mavConf = this.spooky.conf.submodule[MAVConf]
    mavConf.connectRetries = 2
    mavConf.fleet = Fleet.Inventory(simDrones)
  }

  override def beforeAll(): Unit = {
    cleanSweep()

    SpookyUtils.retry(5, 2000) {
      sc.foreachComputer {
        SpookyEnvFixture.processShouldBeClean(Seq("apm"), cleanSweepNotInTask = false)
        SpookyEnvFixture.processShouldBeClean(Seq("python"), cleanSweepNotInTask = false)
        SpookyEnvFixture.processShouldBeClean(Seq("mavproxy"), Seq("mavproxy"), cleanSweepNotInTask = false)
      }
    }

    super.beforeAll()
    val spooky = this.spooky

    val isEmpty = sc.mapPerComputer {APMSim.existing.isEmpty}.collect()
    assert(!isEmpty.contains(false))

    val getNext = this.getNext
    val connStrRDD = sc.parallelize(1 to parallelism)
      .map {
        i =>
          //NOT cleaned by TaskCompletionListener
          val apmSimDriver = new PythonDriver(lifespan = Lifespan.JVM(nameOpt = Some(s"APMSim$i")))
          val sim = getNext()
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
    this.simURIRDD = connStrRDD
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

class APMCopterSITLSuite extends APMSITLFixture {

  import com.tribbloids.spookystuff.utils.SpookyViews._

  /**
    * this test assumes that all test runs on a single machine, so all SITL instance number has to be different
    */
  test("can create many APM instances with different iNum") {
    val iNums = sc.mapPerWorker {
      APMSim.existing.map(_.iNum)
    }
      .collect()
      .toSeq
      .flatMap(_.toSeq)

    println(s"iNums: ${iNums.mkString(", ")}")
    assert(iNums.nonEmpty)
    assert(iNums.size == iNums.distinct.size)

    println(s"connStrs:\n${this.simURIs.mkString("\n")}")
    assert(simURIs.nonEmpty)
    assert(simURIs.size == simURIs.distinct.size)
    assert(simURIs.size == iNums.size)

    import scala.collection.JavaConverters._

    val processes = JProcesses.getProcessList()
      .asScala

    //ensure all apm processes are running
    val apmPs = processes.filter(_.getName == "apm")
    assert(apmPs.size == parallelism)
  }

  //  test("APM instances created with")
}

//class APMPlaneSITLSuite extends APMCopterSITLSuite {
//  override val getNext: () => APMSim = {
//    () =>
//      APMSim.next(extraArgs = Seq(
//        "--gimbal",
//        "--home", APMSim.defaultHomeStr
//      ),
//        vType = "plane", version = "3.3.0")
//  }
//
//}
