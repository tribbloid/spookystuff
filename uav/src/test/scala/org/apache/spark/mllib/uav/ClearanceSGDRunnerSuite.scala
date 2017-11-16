package org.apache.spark.mllib.uav

import com.tribbloids.spookystuff.SpookyEnvFixture
import com.tribbloids.spookystuff.actions.{Trace, TraceView}
import com.tribbloids.spookystuff.execution.SpookyExecutionContext
import com.tribbloids.spookystuff.row.{FetchedRow, SpookySchema}
import com.tribbloids.spookystuff.testutils.AssertSerializable
import com.tribbloids.spookystuff.uav.actions.Waypoint
import com.tribbloids.spookystuff.uav.planning.Traffics.Clearance
import com.tribbloids.spookystuff.uav.spatial.point.NED
import org.apache.spark.rdd.RDD

class ClearanceSGDRunnerSuite extends SpookyEnvFixture {

  lazy val clearance = Clearance()
  val schema = SpookySchema(SpookyExecutionContext(spooky))

  private def map2rdd(input: Map[Int, Seq[Trace]]): RDD[(Int, List[TraceView])] = {
    val schema = this.schema
    val rdd = spooky.sparkContext.parallelize(input.toSeq, input.size)
      .mapValues {
        seq =>
          seq.map{
            v =>
              val interpolated = TraceView(v)
                .interpolateAndRewriteLocally(FetchedRow.Empty, schema).get
              TraceView(interpolated)
          }.toList
      }
    rdd
  }


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
    val runner = ClearanceSGDRunner(map2rdd(input), schema, clearance)
    AssertSerializable(runner.gradient, condition = {(v1: ClearanceGradient, v2: ClearanceGradient) => })
  }

  it("can generate training data data RDD") {
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
    val runner = ClearanceSGDRunner(map2rdd(input), schema, clearance)
    val data = runner.gradient.generateDataRDD
      .collect()

    data.map(_._2.toDense.toBreeze.map(_.toInt))
      .mkString("\n").shouldBe(
      """
        |DenseVector(1, 1, 0)
        |DenseVector(1, 0, 1)
        |DenseVector(0, 1, 1)
      """.stripMargin,
      sort = true
    )
  }

  it("can optimize 2 very close traces") {

    val input: Map[Int, Seq[Trace]] = Map(
      1 -> Seq(List(
        Waypoint(NED(0,0,-0.1)),
        Waypoint(NED(1,1,-0.1))
      )),
      2 -> Seq(List(
        Waypoint(NED(1,0,0.1)),
        Waypoint(NED(0,1,0.1))
      ))
    )
    val runner = ClearanceSGDRunner(map2rdd(input), schema, clearance)

    val output = runner.pid2Traces_flatten
    output.foreach(v => println(v))
  }

  it("can optimize 2 very close unbalanced traces") {

    val input: Map[Int, Seq[Trace]] = Map(
      1 -> Seq(List(
        Waypoint(NED(0,0,-0.3)),
        Waypoint(NED(1,5,-0.3))
      )),
      2 -> Seq(List(
        Waypoint(NED(1,0,0.3)),
        Waypoint(NED(0,1,0.3))
      ))
    )
    val runner = ClearanceSGDRunner(map2rdd(input), schema, clearance)

    val output = runner.pid2Traces_flatten
    output.foreach(v => println(v))
  }

  it("can optimize 2 intersecting traces") {

    val input: Map[Int, Seq[Trace]] = Map(
      1 -> Seq(List(
        Waypoint(NED(0,0,0)),
        Waypoint(NED(1,1,0))
      )),
      2 -> Seq(List(
        Waypoint(NED(1,0,0)),
        Waypoint(NED(0,1,0))
      ))
    )
    val runner = ClearanceSGDRunner(map2rdd(input), schema, clearance)

    val output = runner.pid2Traces_flatten
    output.foreach(v => println(v))
  }

  it("can optimize 2 scissoring traces") {

    val input: Map[Int, Seq[Trace]] = Map(
      1 -> Seq(List(
        Waypoint(NED(0,0,0)),
        Waypoint(NED(1,1,0)),
        Waypoint(NED(0,2,0))
      )),
      2 -> Seq(List(
        Waypoint(NED(1,0,0)),
        Waypoint(NED(0,1,0)),
        Waypoint(NED(1,2,0))
      ))
    )
    val runner = ClearanceSGDRunner(map2rdd(input), schema, clearance)

    val output = runner.pid2Traces_flatten
    output.foreach(v => println(v))
  }

  it("can optimize 4 Waypoints in 2 partitions") {

    val input: Map[Int, Seq[Trace]] = Map(
      1 -> Seq(
        List(
          Waypoint(NED(0,0,0))
        ),
        List(
          Waypoint(NED(1,1,0))
        )),
      2 -> Seq(
        List(
          Waypoint(NED(1,0,0))
        ),
        List(
          Waypoint(NED(0,1,0))
        )
      )
    )
    val runner = ClearanceSGDRunner(map2rdd(input), schema, clearance)

    val output = runner.pid2Traces_flatten
    output.foreach(v => println(v))
  }
}
