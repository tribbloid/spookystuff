package com.tribbloids.spookystuff.uav.sim

import org.jutils.jprocesses.JProcesses

trait APMQuadFixture extends APMFixture {

  lazy val simFactory = APMQuadSimFactory()
}

class TestAPMQuad extends APMQuadFixture {

  import com.tribbloids.spookystuff.utils.SpookyViews._

  /**
    * this test assumes that all test runs on a single machine, so all SITL instance number has to be different
    */
  it("can create many APM instances with different iNum") {
    val iNums = sc.runEverywhere(alsoOnDriver = false) {
      _ =>
        APMSim.existing.map(_.iNum)
    }
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
