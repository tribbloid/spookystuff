package com.tribbloids.spookystuff.execution

import com.tribbloids.spookystuff.actions.*
import com.tribbloids.spookystuff.dsl.Locality
import com.tribbloids.spookystuff.row.*
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object FetchPlan {
  // TODO: HasTraceSet will be gone, Agent can be manipulated directly
  // this will make it definitionally close to FlatMapPlan, the only difference is the shuffling

  type Batch[O] = Seq[(Trace, O)]

  type Fn[I, O] = AgentRow[I] => Batch[O]
}

/**
  * Created by peng on 27/03/16
  *
  * TODO: the only difference between this and [[FlatMapPlan]] is the groupByKey step for sharing locality group, this
  * is too complex. the 2 classes should be merged
  */
case class FetchPlan[I, O: ClassTag](
    override val child: ExecutionPlan[I],
    fn: FetchPlan.Fn[I, O],
    sameBy: Trace => Any,
    locality: Locality
) extends UnaryPlan[I, O](child)
    with CanInjectBeaconRDD[O]
//    with CanChain[O] // TODO: remove, no advantage after rewriting
    {

  override def prepare: SquashedRDD[O] = {

    val sketched: RDD[(LocalityGroup, (O, Int))] = child.squashedRDD
      .flatMap { (v: SquashedRow[I]) =>
        val rows = v.withCtx(child.ctx).unSquash

        val seq = rows.flatMap(fn).zipWithIndex

        seq.map {
          case ((trace, v), i) =>
            LocalityGroup(trace).sameBy(sameBy) -> (v -> i)
        }
      }

    val grouped: RDD[(LocalityGroup, Iterable[(O, Int)])] = gpImpl.groupByKey(sketched, beaconRDDOpt)

    grouped
      .map { tuple =>
        SquashedRow(
          tuple._1,
          tuple._2.toVector
        )

        // actual fetch can only be triggered by extract or savePages
      }
  }

//  override def chain[O2: ClassTag](fn: ChainPlan.Fn[O, O2]): FetchPlan[I, O2] = {
//
//    val newFn: FetchPlan.Fn[I, O2] = { row =>
//      val out1 = this.fn(row)
//
//      val out2: Seq[(Trace, O2)] = out1.flatMap {
//        case (traces, data) =>
//          val row2 = row.copy(data = data)
//
//          fn(row2).map { v =>
//            traces -> v
//          }
//      }
//
//      out2
//    }
//
//    this.copy(
//      fn = newFn
//    )
//  }
}
