package org.apache.spark.mllib.uav

import com.tribbloids.spookystuff.actions.TraceView
import com.tribbloids.spookystuff.row.DataRowSchema
import com.tribbloids.spookystuff.uav.planning.CollisionAvoidances.Clearance
import org.apache.spark.mllib.optimization.{GradientDescent, SquaredL2Updater}
import org.apache.spark.rdd.RDD

import scala.collection.immutable

/**
  *
  * @param schema
  */
case class ClearanceSGDRunner(
                               @transient pid2TracesRDD: RDD[(Int, List[TraceView])],
                               schema: DataRowSchema,
                               outer: Clearance
                             ) {

  val pid2Traces: Map[Int, Seq[TraceView]] = pid2TracesRDD.collectAsMap()
    .toMap.map(identity)

  val gradient = ClearanceGradient(this)
  val updater = new SquaredL2Updater()

  //TODO: result may be a very large object that requires shipping
  //should optimize after PoC
  @transient lazy val conversion: Seq[(TraceView, TraceView)] = {

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
    val pid2TracesRDD_shifted: Map[Int, Seq[TraceView]] = gradient.id2VectorIndexedTrace
      .mapValues {
        array =>
          array.map {
            trace =>
              val shifted = trace.map {
                action =>
                  val shifted = action match {
                    case v: NavFeatureEncoding =>
                      v.shiftAllByWeight(weights_brz)
                    case _ =>
                      action
                  }
                  shifted
              }
              TraceView(shifted)
          }
      }
    val result: Seq[(TraceView, TraceView)] = {
      val list: immutable.Iterable[Seq[(TraceView, TraceView)]] = for (entry <- pid2Traces) yield {
        val before = entry._2
        val after = pid2TracesRDD_shifted(entry._1)
        before.zipAll(after, null, null)
      }

      list.flatten.toSeq
    }
    result
  }

  @transient lazy val conversionMap = Map(conversion: _*)
  @transient lazy val conversionMap_broadcast = schema.spooky.sparkContext.broadcast(conversionMap)

  @transient lazy val pid2Traces_converted: Map[Int, Seq[TraceView]] = pid2Traces.mapValues {
    v =>
      v.map(vv => conversionMap(vv))
  }

  @transient lazy val pid2Traces_flatten: List[(Int, TraceView)] = pid2Traces_converted.toList.flatMap {
    case (i, v) =>
      v.map {
        vv =>
          i -> vv
      }
  }

  //  def solved: RDD[(TraceView, TraceView)] = {
  //    val solvedMappings = this.conversion
  //    val solvedMappingsRDD: RDD[(TraceView, TraceView)] = schema.spooky.sparkContext
  //      .parallelize(solvedMappings)
  //
  //    schema.ec.scratchRDDs.persist(rdd, schema.spooky.spookyConf.defaultStorageLevel)
  //    val locality = LocalityRDDView(rdd)
  //
  //    val semiCogrouped: RDD[(TraceView, (V, Iterable[TraceView]))] = locality
  //      .cogroupBase(solvedMappingsRDD)
  //
  //    val result: RDD[(TraceView, V)] = semiCogrouped
  //      .values
  //      .map {
  //        v =>
  //          assert(v._2.size == 1, "IMPOSSIBLE!")
  //          v._2.head -> v._1
  //      }
  //
  //    result
  //  }
}
