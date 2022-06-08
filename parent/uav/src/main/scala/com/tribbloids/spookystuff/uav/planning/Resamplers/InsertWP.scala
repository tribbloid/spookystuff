package com.tribbloids.spookystuff.uav.planning.Resamplers

import com.tribbloids.spookystuff.actions.{Action, Trace}
import com.tribbloids.spookystuff.row.SpookySchema
import com.tribbloids.spookystuff.uav.actions.UAVNavigation
import com.tribbloids.spookystuff.uav.planning.TrafficControls.Avoid
import com.tribbloids.spookystuff.uav.planning.{Resampler, ResamplerInst, TrafficControl}
import org.apache.spark.ml.uav.TwoLines

case class InsertWP(
    granularity: Double = 1.0
) extends Resampler {

  def apply(gp: TrafficControl, schema: SpookySchema) = Inst(gp, schema)

  case class Inst(gp: TrafficControl, schema: SpookySchema) extends ResamplerInst {

    lazy val trafficClearance = gp match {
      case v: Avoid => v._traffic
      case _        => 0.0
    }

    def apply(v: Map[Int, Seq[Trace]]): Map[Int, Seq[Trace]] = {

      val indexed: Map[Int, Seq[List[(Action, (Int, Int))]]] = v.mapValues { seq =>
        val flattenWIndices: Seq[List[(Action, (Int, Int))]] = seq.zipWithIndex.map {
          case (trace, i) =>
            trace.zipWithIndex.map {
              case (vv, j) =>
                vv -> (i -> j)
            }
        }
        flattenWIndices
      }

      val resamplingBuffers: Map[Int, Seq[ResamplingBuffer]] = indexed
        .mapValues { ss =>
          val navs = ss.flatten.collect {
            case (nav: UAVNavigation, i) => nav.WSchema(schema) -> i
          }
          val result = navs.indices.dropRight(1).map { i =>
            ResamplingBuffer(navs(i), navs(i + 1))
          }
          result
        }
        .map(identity) //don't detete! or only MapView remains, and buffer will be recreated, scala is not cool

      val keys = resamplingBuffers.keys
      for (i <- keys;
           j <- keys) {
        if (i != j) {
          val iLines = resamplingBuffers(i)
          val jLines = resamplingBuffers(j)

          for (iLine <- iLines;
               jLine <- jLines) {
            val twoLines = TwoLines(iLine.from._1, iLine.to._1, jLine.from._1, jLine.to._1)
            import twoLines._

            val violation = trafficClearance - D

            if (violation > 0) {
              iLine.ts += t1
              jLine.ts += t2
            }
          }
        }
      }

      val replacements: Map[Int, Map[(Int, Int), Seq[UAVNavigation]]] = resamplingBuffers.mapValues { seq =>
        val list = seq.map { v =>
          val index = v.to._2
          val replacement = v.WParams().getResult :+ v.to._1.outer
          index -> replacement
        }
        val map = Map(list: _*)
        map
      }

      val results: Map[Int, Seq[Trace]] = indexed.map {
        case (k, ss) =>
          val replacement = replacements(k)
          val partition = ss.map { s =>
            val trace = s.flatMap {
              case (action, index) =>
                val actions: Seq[Action] = replacement.getOrElse(index, Seq(action))
                actions
            }
            trace
          }
          k -> partition
      }
      results
    }
  }
}
