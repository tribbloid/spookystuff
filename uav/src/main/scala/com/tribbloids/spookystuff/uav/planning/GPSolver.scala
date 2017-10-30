package com.tribbloids.spookystuff.uav.planning

import com.tribbloids.spookystuff.actions.TraceView
import com.tribbloids.spookystuff.dsl.GenPartitioner
import com.tribbloids.spookystuff.row.DataRowSchema
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

case class TraceMapping(
                           old: TraceView,
                           neo: TraceView
                           )

trait GPSolver[T <: GenPartitioner] extends Serializable {

  def solve[V: ClassTag](
                          gp: T,
                          schema: DataRowSchema,
                          rdd: RDD[(TraceView, V)]
                        ): RDD[(TraceView, V)] = {

    schema.ec.scratchRDDs.persist(rdd)
    val keyRDD = rdd.keys
    val mapping = _solve(gp, schema, keyRDD)

//    val mapping =
    ???
  }

  /**
    * a much more lightweight API, subclass only needs to define partitioning and ordering of keys.
    * @param gp
    * @param schema
    * @param rdd
    * @return
    */
  def _solve(
              gp: T,
              schema: DataRowSchema,
              rdd: RDD[TraceView]
            ): RDD[Seq[TraceMapping]] = {

    
  }
}
