package org.apache.spark.mllib.uav

import com.tribbloids.spookystuff.actions.{Trace, TraceView}
import com.tribbloids.spookystuff.row.SpookySchema
import com.tribbloids.spookystuff.uav.planning.Traffics.Clearance
import org.apache.spark.mllib.optimization.{GradientDescent, SquaredL2Updater}
import org.apache.spark.rdd.RDD

import scala.collection.immutable

case class ClearanceSGDRunner(
                               pid2TracesRDD: RDD[(Int, List[TraceView])],
                               schema: SpookySchema,
                               outer: Clearance
                             ) {

  val pid2Traces: Map[Int, Seq[Trace]] = pid2TracesRDD
    .mapValues {
      seq =>
        seq.map(_.children)
    }
    .collectAsMap()
    .toMap.map(identity)

  val pid2Traces_resampled = outer.resampler match {
    case None =>
      pid2Traces
    case Some(v) =>
      v.apply(outer, schema).apply(pid2Traces)
  }

  val gradient = ClearanceGradient(this)
  val updater = new SquaredL2Updater()

  //TODO: result may be a very large object that requires shipping
  //should optimize after PoC
  lazy val conversion: Seq[(Trace, Trace)] = {

    val data = gradient.generateDataRDD
    val (weights, convergence) = GradientDescent.runMiniBatchSGD(
      data = data,
      gradient = gradient,
      updater = updater,
      stepSize = 1.0,
      numIterations = 50,
      regParam = 0.1,
      miniBatchFraction = 1.0,
      initialWeights = gradient.initialWeights
    )
    val weights_brz = weights.toBreeze
    val pid2TracesRDD_shifted: Map[Int, Seq[Trace]] = gradient
      .id2Traces_withEncoded
      .mapValues {
        array =>
          array.map {
            trace =>
              val shifted = trace.map {
                action =>
                  val shifted = action match {
                    case v: VectorEncodedNav =>
                      v.shiftByWeights(weights_brz).self.outer
                    case _ =>
                      action
                  }
                  shifted
              }
              shifted
          }
      }
    val result: Seq[(Trace, Trace)] = {
      val list: immutable.Iterable[Seq[(Trace, Trace)]] = for (entry <- pid2Traces_resampled) yield {
        val before = entry._2
        val after = pid2TracesRDD_shifted(entry._1)
        before.zipAll(after, null, null)
      }

      list.flatten.toSeq
    }
    result
  }

  lazy val conversionMap = Map(conversion: _*)
  lazy val conversionMap_broadcast = schema.spooky.sparkContext.broadcast(conversionMap)

  // after this line, for test only
  lazy val pid2Traces_converted: Map[Int, Seq[Trace]] = pid2Traces_resampled.mapValues {
    v =>
      v.map(vv => conversionMap(vv))
  }

  lazy val pid2Traces_flatten: List[(Int, Trace)] = pid2Traces_converted.toList.flatMap {
    case (i, v) =>
      v.map {
        vv =>
          i -> vv
      }
  }
}
