package org.apache.spark.mllib.uav

import com.tribbloids.spookystuff.actions.{Trace, TraceView}
import com.tribbloids.spookystuff.row.FetchedRow
import com.tribbloids.spookystuff.uav.actions.Waypoint
import com.tribbloids.spookystuff.uav.planning.TrafficControls.Avoid
import com.tribbloids.spookystuff.uav.spatial.point.NED
import com.tribbloids.spookystuff.utils.{AssertSerializable, TreeException}
import org.apache.spark.ml.uav.{AvoidGradient, AvoidSGDRunner}
import org.apache.spark.rdd.RDD

import scala.util.Try

class AvoidSGDRunnerSuite extends GeomMixin {

  lazy val clearance = Avoid()

  private def map2rdd(input: Map[Int, Seq[Trace]]): RDD[(Int, List[TraceView])] = {
    val schema = this.schema
    val rdd = spooky.sparkContext
      .parallelize(input.toSeq, input.size)
      .mapValues { seq =>
        seq.map { v =>
          val interpolated = TraceView(v)
            .interpolateAndRewriteLocally(FetchedRow.Empty, schema)
            .get
          TraceView(interpolated)
        }.toList
      }
    rdd
  }

  it("ClearanceGradient is serializable") {
    val input: Map[Int, Seq[Trace]] = Map(
      1 -> Seq(
        List(
          Waypoint(NED(0, 0, 0)),
          Waypoint(NED(1, 1, 0))
        )),
      2 -> Seq(
        List(
          Waypoint(NED(1, 0, 0.1)),
          Waypoint(NED(0, 1, 0.1))
        ))
    )
    val runner = AvoidSGDRunner(map2rdd(input), schema, clearance)
    AssertSerializable(runner.gradient, condition = { (v1: AvoidGradient, v2: AvoidGradient) =>
      })
  }

  it("can generate training data RDD") {
    val input: Map[Int, Seq[Trace]] = Map(
      1 -> Seq(
        List(
          Waypoint(NED(0, 0, 0)),
          Waypoint(NED(1, 1, 0))
        )),
      2 -> Seq(
        List(
          Waypoint(NED(1, 0, 0.1)),
          Waypoint(NED(0, 1, 0.1))
        )),
      3 -> Seq(
        List(
          Waypoint(NED(0, 0, 0.1)),
          Waypoint(NED(0, 1, 0.1))
        ))
    )
    val runner = AvoidSGDRunner(map2rdd(input), schema, clearance)
    val data = runner.gradient.generateDataRDD
      .collect()

    data
      .map(_._2.toDense.asBreeze.map(_.toInt))
      .mkString("\n")
      .shouldBe(
        """
        |DenseVector(1, 1, 0)
        |DenseVector(1, 0, 1)
        |DenseVector(0, 1, 1)
      """.stripMargin,
        sort = true
      )
  }

  def expected1 =
    """
      |NED:POINT (0 0 0.535923)	NED:POINT (1 1 0.536389)
      |NED:POINT (0 1 -0.534443)	NED:POINT (1 0 -0.534677)
    """.stripMargin
  it("can optimize 2 very close traces") {

    val input: Map[Int, Seq[Trace]] = Map(
      1 -> Seq(
        List(
          Waypoint(NED(0, 0, -0.1)),
          Waypoint(NED(1, 1, -0.1))
        )),
      2 -> Seq(
        List(
          Waypoint(NED(1, 0, 0.1)),
          Waypoint(NED(0, 1, 0.1))
        ))
    )

    val runner = AvoidSGDRunner(map2rdd(input), schema, clearance)

    val output = runner.traces_flatten
    validateTraces(output, expected1)
  }

  def expected2 =
    """
      |NED:POINT (0 0 0.554847)	NED:POINT (5 1 0.351919)
      |NED:POINT (0 1 -0.348346)	NED:POINT (1 0 -0.550647)
    """.stripMargin
  it("can optimize 2 very close unbalanced traces") {

    val input: Map[Int, Seq[Trace]] = Map(
      1 -> Seq(
        List(
          Waypoint(NED(0, 0, -0.3)),
          Waypoint(NED(1, 5, -0.3))
        )),
      2 -> Seq(
        List(
          Waypoint(NED(1, 0, 0.3)),
          Waypoint(NED(0, 1, 0.3))
        ))
    )

    val runner = AvoidSGDRunner(map2rdd(input), schema, clearance)

    val output = runner.traces_flatten
    validateTraces(output, expected2)
  }

  def expected3 =
    """
      |NED:POINT (0 0 -0.521465)	NED:POINT (1 1 -0.521376)
      |NED:POINT (0 1 0.523265)	NED:POINT (1 0 0.523709)
      |---
      |NED:POINT (0 0 0.521465)	NED:POINT (1 1 0.521376)
      |NED:POINT (0 1 -0.523265)	NED:POINT (1 0 -0.523709)
    """.stripMargin
  it("can optimize 2 intersecting traces") {

    val input: Map[Int, Seq[Trace]] = Map(
      1 -> Seq(
        List(
          Waypoint(NED(0, 0, 0)),
          Waypoint(NED(1, 1, 0))
        )),
      2 -> Seq(
        List(
          Waypoint(NED(1, 0, 0)),
          Waypoint(NED(0, 1, 0))
        ))
    )

    val runner = AvoidSGDRunner(map2rdd(input), schema, clearance)

    val output = runner.traces_flatten
    TreeException.|||(
      Seq(
        Try {}
      )
    )
    validateTraces(output, expected3)
  }

  def expected4 =
    """
      |NED:POINT (0 0 -0.402375)	NED:POINT (1 1 -0.705099)	NED:POINT (2 0 -0.40141)
      |NED:POINT (0 1 0.404129)	NED:POINT (1 0 0.708362)	NED:POINT (2 1 0.404958)
    """.stripMargin
  it("can optimize 2 scissoring traces") {

    val input: Map[Int, Seq[Trace]] = Map(
      1 -> Seq(
        List(
          Waypoint(NED(0, 0, 0.1)),
          Waypoint(NED(1, 1, 0.1)),
          Waypoint(NED(0, 2, 0.1))
        )),
      2 -> Seq(
        List(
          Waypoint(NED(1, 0, -0.1)),
          Waypoint(NED(0, 1, -0.1)),
          Waypoint(NED(1, 2, -0.1))
        ))
    )

    val runner = AvoidSGDRunner(map2rdd(input), schema, clearance)

    val output = runner.traces_flatten
    validateTraces(output, expected4)
  }

  def expected5 =
    """
      |NED:POINT (0 0 -0.50103)
      |NED:POINT (1 1 -0.50116)
      |NED:POINT (0 1 0.503893)
      |NED:POINT (1 0 0.50409)
    """.stripMargin
  it("can optimize 4 Waypoints in 2 partitions") {

    val input: Map[Int, Seq[Trace]] = Map(
      1 -> Seq(List(
                 Waypoint(NED(0, 0, 0.1))
               ),
               List(
                 Waypoint(NED(1, 1, 0.1))
               )),
      2 -> Seq(
        List(
          Waypoint(NED(1, 0, -0.1))
        ),
        List(
          Waypoint(NED(0, 1, -0.1))
        )
      )
    )

    val runner = AvoidSGDRunner(map2rdd(input), schema, clearance)

    val output = runner.traces_flatten
    validateTraces(output, expected5)
  }
}
