package org.apache.spark.mllib.uav

import com.tribbloids.spookystuff.SpookyEnvFixture
import com.tribbloids.spookystuff.actions.Trace
import com.tribbloids.spookystuff.execution.SpookyExecutionContext
import com.tribbloids.spookystuff.row.SpookySchema
import com.tribbloids.spookystuff.uav.UAVConf
import com.tribbloids.spookystuff.uav.actions.UAVNavigation
import com.tribbloids.spookystuff.uav.spatial.Spatial
import com.tribbloids.spookystuff.uav.spatial.point.NED
import com.tribbloids.spookystuff.utils.TreeThrowable

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

trait GeomMixin extends SpookyEnvFixture {

  val schema = SpookySchema(SpookyExecutionContext(spooky))

  def validateTraces(
      actual: Iterable[Trace],
      expected: String = null,
      mseCap: Double = 0.1
  ) = {

    val ss = actual.toList.map { trace =>
      val navs = trace.collect {
        case v: UAVNavigation => v
      }
      val coordinates = navs.map { nav =>
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

    val actualRepr = actual
      .map { cs =>
        cs.mkString("\t")
      }
      .mkString("\n")

    Option(expected) match {
      case None =>
        actualRepr.shouldBe()
      case Some(gds) =>
        val gdSeq = gds.split("---")
        val originalErrors = ArrayBuffer.empty[Throwable]

        val trials = gdSeq.map { gd =>
          Try {
            val gdParsed: Seq[Seq[Spatial]] = gd.trim
              .split("\n")
              .map {
                _.split("\t").toSeq.map { str =>
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
          }.recoverWith {
            case e: Exception =>
              originalErrors += e

              Try { actualRepr.shouldBe(gd) }
          }
        }
        TreeThrowable.|||(trials.toSeq, extra = originalErrors)
    }
  }
}
