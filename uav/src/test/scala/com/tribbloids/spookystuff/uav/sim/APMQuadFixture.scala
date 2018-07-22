package com.tribbloids.spookystuff.uav.sim

import com.tribbloids.spookystuff.SpookyEnvFixture
import com.tribbloids.spookystuff.session.python.PythonDriver
import com.tribbloids.spookystuff.uav.SimUAVFixture
import com.tribbloids.spookystuff.uav.telemetry.Link
import com.tribbloids.spookystuff.utils.CommonUtils
import com.tribbloids.spookystuff.utils.lifespan.{Cleanable, Lifespan}
import org.apache.spark.rdd.RDD
import org.jutils.jprocesses.JProcesses
import org.slf4j.LoggerFactory

trait APMFixture extends SimUAVFixture {

  import com.tribbloids.spookystuff.utils.SpookyViews._

  def simFactory: SimFactory

  override val processNames = Seq("phantomjs", "python", "apm")

  private var _simURIRDD: RDD[String] = _
  lazy val fleetURIs: Seq[String] = {

    val simFactory = this.simFactory
    this._simURIRDD = sc.parallelize(1 to parallelism)
      .map {
        i =>
          //NOT cleaned by TaskCompletionListener
          val apmSimDriver = new PythonDriver(_lifespan = Lifespan.JVM(nameOpt = Some(s"APMSim$i")))
          val sim = simFactory.getNext
          sim._Py(apmSimDriver).connStr.$STR
      }
      .flatMap(v => v)
      .persist()

    val result = _simURIRDD.collect().toSeq
    assert(result.size == result.distinct.size)
    val info = result.mkString("\n")
    LoggerFactory.getLogger(this.getClass).info(
      s"""
         |APM simulation(s) are up and running:
         |$info
      """.stripMargin
    )
    result
  }

  override def beforeAll(): Unit = {
    cleanSweep()

    Thread.sleep(2000)
    // small delay added to ensure that cleanSweep
    // won't accidentally clean object created in the suite

    CommonUtils.retry(5, 2000) {
      sc.foreachComputer {
        SpookyEnvFixture.processShouldBeClean(Seq("apm"), cleanSweepNotInTask = false)
        SpookyEnvFixture.processShouldBeClean(Seq("python"), cleanSweepNotInTask = false)
        SpookyEnvFixture.processShouldBeClean(Seq("mavproxy"), Seq("mavproxy"), cleanSweepNotInTask = false)
      }
    }

    super.beforeAll()

    val isEmpty = sc.mapPerComputer {APMSim.existing.isEmpty}.collect()
    assert(!isEmpty.contains(false))
  }

  override def afterAll(): Unit = {
    cleanSweep()
    Option(this._simURIRDD).foreach(_.unpersist())
    super.afterAll()
  }

  private def cleanSweep() = {
    sc.foreachComputer {
      // in production Links will be cleaned up by shutdown hook

      APMFixture.cleanSweepLocally()
      Predef.assert(
        Link.registered.isEmpty,
        Link.registered.keys.mkString("\n")
      )
    }
  }

}

object APMFixture {

  def cleanSweepLocally() = {
    Cleanable.cleanSweepAll {
      case _: Link => true
      case _: APMSim => true
      case _ => false
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

    println(s"connStrs:\n${this.fleet.mkString("\n")}")
    assert(fleet.nonEmpty)
    assert(fleet.size == fleet.distinct.size)
    assert(fleet.size == iNums.size)

    import scala.collection.JavaConverters._

    val processes = JProcesses.getProcessList()
      .asScala

    //ensure all apm processes are running
    val apmPs = processes.filter(_.getName == "apm")
    assert(apmPs.size == parallelism)
  }
}

trait APMQuadFixture extends APMFixture {

  lazy val simFactory = APMQuadSimFactory()
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
