package com.tribbloids.spookystuff.uav.sim

import com.tribbloids.spookystuff.SpookyEnvFixture
import com.tribbloids.spookystuff.session.python.PythonDriver
import com.tribbloids.spookystuff.session.{Cleanable, Lifespan}
import com.tribbloids.spookystuff.uav.UAVFixture
import com.tribbloids.spookystuff.uav.telemetry.Link
import com.tribbloids.spookystuff.utils.SpookyUtils
import org.jutils.jprocesses.JProcesses
import org.slf4j.LoggerFactory

trait APMFixture extends UAVFixture {

  import com.tribbloids.spookystuff.utils.SpookyViews._

  def simFactory: SimFactory

  override val processNames = Seq("phantomjs", "python", "apm")

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

    val isEmpty = sc.mapPerComputer {APMSim.existing.isEmpty}.collect()
    assert(!isEmpty.contains(false))

    val simFactory = this.simFactory
    val connStrRDD = sc.parallelize(1 to parallelism)
      .map {
        i =>
          //NOT cleaned by TaskCompletionListener
          val apmSimDriver = new PythonDriver(lifespan = Lifespan.JVM(nameOpt = Some(s"APMSim$i")))
          val sim = simFactory.getNext
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

class TestAPMQuad extends APMQuadFixture {

  import com.tribbloids.spookystuff.utils.SpookyViews._

  /**
    * this test assumes that all test runs on a single machine, so all SITL instance number has to be different
    */
  it("can create many APM instances with different iNum") {
    val iNums = sc.mapPerWorker {
      APMSim.existing.map(_.iNum)
    }
      .collect()
      .toSeq
      .flatMap(_.toSeq)

    println(s"iNums: ${iNums.mkString(", ")}")
    assert(iNums.nonEmpty)
    assert(iNums.size == iNums.distinct.size)

    println(s"connStrs:\n${this.simUAVs.mkString("\n")}")
    assert(simUAVs.nonEmpty)
    assert(simUAVs.size == simUAVs.distinct.size)
    assert(simUAVs.size == iNums.size)

    import scala.collection.JavaConverters._

    val processes = JProcesses.getProcessList()
      .asScala

    //ensure all apm processes are running
    val apmPs = processes.filter(_.getName == "apm")
    assert(apmPs.size == parallelism)
  }
}

trait APMQuadFixture extends APMFixture {

  lazy val simFactory = QuadSimFactory()
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
