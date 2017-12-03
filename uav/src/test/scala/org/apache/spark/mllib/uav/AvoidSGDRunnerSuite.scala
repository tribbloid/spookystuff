package org.apache.spark.mllib.uav

import com.tribbloids.spookystuff.SpookyEnvFixture
import com.tribbloids.spookystuff.actions.{Trace, TraceView}
import com.tribbloids.spookystuff.execution.SpookyExecutionContext
import com.tribbloids.spookystuff.row.{FetchedRow, SpookySchema}
import com.tribbloids.spookystuff.testutils.AssertSerializable
import com.tribbloids.spookystuff.uav.UAVConf
import com.tribbloids.spookystuff.uav.actions.{UAVNavigation, Waypoint}
import com.tribbloids.spookystuff.uav.planning.TrafficControls.Avoid
import com.tribbloids.spookystuff.uav.spatial.Spatial
import com.tribbloids.spookystuff.uav.spatial.point.NED
import com.tribbloids.spookystuff.utils.TreeException
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

class AvoidSGDRunnerSuite extends SpookyEnvFixture {

  lazy val clearance = Avoid()
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
    val runner = AvoidSGDRunner(map2rdd(input), schema, clearance)
    AssertSerializable(runner.gradient, condition = {(v1: AvoidGradient, v2: AvoidGradient) => })
  }

  it("can generate training data RDD") {
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
    val runner = AvoidSGDRunner(map2rdd(input), schema, clearance)
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

  def validateTraces(
                      actual: Iterable[Trace],
                      expected: String = null,
                      mseCap: Double = 0.1
                    ) = {

    val ss = actual.toList.map {
      trace =>
        val navs = trace.collect {
          case v: UAVNavigation => v
        }
        val coordinates = navs.map {
          nav =>
            nav.getLocation(schema).coordinate(NED, spooky.getConf[UAVConf].home)
        }
        coordinates
    }
    validateGeoms(ss, expected, mseCap)
  }

  def validateGeoms(
                     actual: Seq[Seq[Spatial]],
                     expected: String = null,
                     mseCap: Double = 0.1
                   ): Unit = {

    val actualRepr = actual.map {
      cs =>
        cs.mkString("\t")
    }
      .mkString("\n")

    Option(expected) match {
      case None =>
        actualRepr.shouldBe()
      case Some(gds) =>
        val gdSeq = gds.split("---")
        val originalErrors = ArrayBuffer.empty[Throwable]

        val trials = gdSeq.map {
          gd =>
            Try {
              val gdParsed: Seq[Seq[Spatial]] = gd.trim.split("\n")
                .map {
                  _.split("\t").toSeq.map {
                    str =>
                      Spatial.parse(str)
                  }
                }
                .toSeq
              val ses: Seq[Seq[Double]] = gdParsed.zipAll(actual, null, null).map {
                case (seq1, seq2) =>
                  val zipped = seq1.zipAll(seq2, null, null)
                  zipped.map {
                    case (ee, aa) =>
                      val v1 = ee.vector
                      val v2 = aa.vector
                      val diff = v1 - v2
                      diff.t * diff
                  }
              }

              val mse = ses.map(_.sum).sum
              assert(mse < mseCap, s"MSE = $mse")
            }
              .recoverWith {
                case e: Throwable =>
                  originalErrors += e

                  Try{actualRepr.shouldBe(gd)}
              }
        }
        TreeException.|||(trials.toSeq, extra = originalErrors)
    }
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
    val expected =
      """
        |NED:POINT (0 0 0.535923)	NED:POINT (1 1 0.536389)
        |NED:POINT (0 1 -0.534443)	NED:POINT (1 0 -0.534677)
      """.stripMargin

    val runner = AvoidSGDRunner(map2rdd(input), schema, clearance)

    val output = runner.traces_flatten
    validateTraces(output, expected)
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
    val expected =
      """
        |NED:POINT (0 0 0.554847)	NED:POINT (5 1 0.351919)
        |NED:POINT (0 1 -0.348346)	NED:POINT (1 0 -0.550647)
      """.stripMargin

    val runner = AvoidSGDRunner(map2rdd(input), schema, clearance)

    val output = runner.traces_flatten
    validateTraces(output, expected)
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
    val expected =
      """
        |NED:POINT (0 0 -0.521465)	NED:POINT (1 1 -0.521376)
        |NED:POINT (0 1 0.523265)	NED:POINT (1 0 0.523709)
        |---
        |NED:POINT (0 0 0.521465)	NED:POINT (1 1 0.521376)
        |NED:POINT (0 1 -0.523265)	NED:POINT (1 0 -0.523709)
      """.stripMargin

    val runner = AvoidSGDRunner(map2rdd(input), schema, clearance)

    val output = runner.traces_flatten
    TreeException.|||(
      Seq(
        Try{}
      )
    )
    validateTraces(output, expected)
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
    val expected =
      """
        |NED:POINT (0 0 0.475764)	NED:POINT (1 1 0.779836)	NED:POINT (2 0 0.477178)
        |NED:POINT (0 1 -0.474217)	NED:POINT (1 0 -0.777852)	NED:POINT (2 1 -0.473402)
        |---
        |NED:POINT (0 0 -0.475764)	NED:POINT (1 1 -0.779836)	NED:POINT (2 0 -0.477178)
        |NED:POINT (0 1 0.474217)	NED:POINT (1 0 0.777852)	NED:POINT (2 1 0.473402)
      """.stripMargin

    val runner = AvoidSGDRunner(map2rdd(input), schema, clearance)

    val output = runner.traces_flatten
    validateTraces(output, expected)
  }

  it("can optimize 4 Waypoints in 2 partitions") {

    val input: Map[Int, Seq[Trace]] = Map(
      1 -> Seq(
        List(
          Waypoint(NED(0,0,0.1))
        ),
        List(
          Waypoint(NED(1,1,0.1))
        )),
      2 -> Seq(
        List(
          Waypoint(NED(1,0,-0.1))
        ),
        List(
          Waypoint(NED(0,1,-0.1))
        )
      )
    )
    val expected =
      """
        |NED:POINT (0 0 -0.50103)
        |NED:POINT (1 1 -0.50116)
        |NED:POINT (0 1 0.503893)
        |NED:POINT (1 0 0.50409)
      """.stripMargin

    val runner = AvoidSGDRunner(map2rdd(input), schema, clearance)

    val output = runner.traces_flatten
    validateTraces(output, expected)
  }
}
