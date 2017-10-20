package org.apache.spark.mllib.uav

import com.tribbloids.spookystuff.SpookyEnvFixture
import com.tribbloids.spookystuff.actions.Trace
import com.tribbloids.spookystuff.execution.ExecutionContext
import com.tribbloids.spookystuff.row.DataRowSchema
import com.tribbloids.spookystuff.testutils.AssertSerializable
import com.tribbloids.spookystuff.uav.UAVConf
import com.tribbloids.spookystuff.uav.actions.Waypoint
import com.tribbloids.spookystuff.uav.planning.traffic.Clearance
import com.tribbloids.spookystuff.uav.spatial.point.NED

class ClearanceRunnerSuite extends SpookyEnvFixture {

  val clearance = Clearance()
  val schema = DataRowSchema(ExecutionContext(spooky))

  it("ClearanceGradient is serializable") {
    val input: Map[Int, Seq[Trace]] = Map(
      1 -> Seq(List(
        Waypoint(NED(0,0,0)),
        Waypoint(NED(1,1,0))
      )),
      2 -> Seq(List(
        Waypoint(NED(1,0,0.1)),
        Waypoint(NED(0,1,0.1))
      ))
    )
    val runner = ClearanceRunner(input, schema, clearance)
    AssertSerializable(runner.gradient, condition = {(v1: ClearanceGradient,v2: ClearanceGradient) => })
  }

  it("can generate data RDD") {
    val input: Map[Int, Seq[Trace]] = Map(
      1 -> Seq(List(
        Waypoint(NED(0,0,0)),
        Waypoint(NED(1,1,0))
      )),
      2 -> Seq(List(
        Waypoint(NED(1,0,0.1)),
        Waypoint(NED(0,1,0.1))
      )),
      3 -> Seq(List(
        Waypoint(NED(0,0,0.1)),
        Waypoint(NED(0,1,0.1))
      ))
    )
    val runner = ClearanceRunner(input, schema, clearance)
    val data = runner.gradient.generateDataRDD
      .collect()

    data.map(_._2.toDense.toBreeze.map(_.toInt))
      .mkString("\n").shouldBe(
      """
        |DenseVector(1, 1, 0)
        |DenseVector(1, 0, 1)
        |DenseVector(0, 1, 1)
      """.stripMargin
    )
  }

  it("can optimize 2 very close traces") {

    val input: Map[Int, Seq[Trace]] = Map(
      1 -> Seq(List(
        Waypoint(NED(0,0,-0.1) -> spooky.getConf[UAVConf].home),
        Waypoint(NED(1,1,-0.1) -> spooky.getConf[UAVConf].home)
      )),
      2 -> Seq(List(
        Waypoint(NED(1,0,0.1) -> spooky.getConf[UAVConf].home),
        Waypoint(NED(0,1,0.1) -> spooky.getConf[UAVConf].home)
      ))
    )
    val runner = ClearanceRunner(input, schema, clearance)
    val output = runner.solve

    output.foreach(println)
  }
}
