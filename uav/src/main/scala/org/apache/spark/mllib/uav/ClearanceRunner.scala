package org.apache.spark.mllib.uav

import com.tribbloids.spookystuff.actions.TraceView
import com.tribbloids.spookystuff.row.DataRowSchema
import com.tribbloids.spookystuff.uav.planning.CollisionAvoidances.Clearance
import org.apache.spark.TaskContext
import org.apache.spark.mllib.optimization.{GradientDescent, SquaredL2Updater}
import org.apache.spark.rdd.RDD

import scala.collection.immutable
import scala.reflect.ClassTag

/**
  *
  * @param schema
  */
case class ClearanceRunner[V: ClassTag](
                                         @transient rdd: RDD[(TraceView, V)],
                                         schema: DataRowSchema,
                                         outer: Clearance
                                       ) {

  import com.tribbloids.spookystuff.utils.SpookyViews._

  val updater = new SquaredL2Updater()

  schema.ec.scratchRDDs.persist(rdd)
  val pid2TracesRDD: RDD[(Int, List[TraceView])] = {
    rdd.keys
      .mapPartitions {
        itr =>
          val result: (Int, List[TraceView]) = TaskContext.get().partitionId() -> itr.toList
          Iterator(result)
      }
  }
  val pid2Traces: Map[Int, Seq[TraceView]] = pid2TracesRDD.collectAsMap().toMap
  val gradient = ClearanceGradient(this)

  //TODO: result may be a very large object that requires shipping
  //should optimize after PoC
  def conversion: Seq[(TraceView, TraceView)] = {
    val data = gradient.generateDataRDD
    val (weights, convergence) = GradientDescent.runMiniBatchSGD(
      data = data,
      gradient = gradient,
      updater = updater,
      stepSize = 1.0,
      numIterations = 50,
      regParam = 0.1,
      miniBatchFraction = 1.0,
      initialWeights = gradient.initializeWeight
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
                    case v: VectorIndexedNav =>
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

  def solved: RDD[(TraceView, V)] = {
    val solvedMappings = this.conversion
    val solvedMappingsRDD: RDD[(TraceView, TraceView)] = schema.spooky.sparkContext
      .parallelize(solvedMappings)

    val semiCogrouped = rdd
      .localityPreservingSemiCogroup(solvedMappingsRDD)

    val result: RDD[(TraceView, V)] = semiCogrouped
      .values
      .map {
        v =>
          assert(v._2.size == 1, "IMPOSSIBLE!")
          v._2.head -> v._1
      }

    result
  }
}
