package com.tribbloids.spookystuff.uav.dsl

import com.tribbloids.spookystuff.actions.TraceView
import com.tribbloids.spookystuff.dsl.GenPartitioner
import com.tribbloids.spookystuff.dsl.GenPartitionerLike.Instance
import com.tribbloids.spookystuff.execution.ExecutionContext
import com.tribbloids.spookystuff.row.BeaconRDD
import com.tribbloids.spookystuff.uav.actions.UAVAction
import com.tribbloids.spookystuff.uav.planning.{JSpritSolver, PreferUAV}
import com.tribbloids.spookystuff.uav.telemetry.{Link, LinkStatus}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * Created by peng on 31/12/16.
  */
object GenPartitioners {

  case class JSprit(
                     preprocessing: GenPartitioner = {
                       com.tribbloids.spookystuff.dsl.GenPartitioners.Wide()
                     },
                     numUAVsOpt: Option[Int] = None,
                     solutionPlotPathOpt: Option[String] = Some("log/solution.png"),
                     progressPlotPathOpt: Option[String] = Some("log/progress.png") // for debugging only.
                   ) extends GenPartitioner {

    def getInstance[K >: TraceView: ClassTag](ec: ExecutionContext): Instance[K] = {
      Inst[K](ec)
    }

    case class Inst[K >: TraceView](ec: ExecutionContext)(
      implicit val ctg: ClassTag[K]
    ) extends Instance[K] {

      //gather all UAVActions to driver and use a local solver (JSprit) to rearrange them.
      override def groupByKey[V: ClassTag](
                                            rdd: RDD[(K, V)],
                                            beaconRDDOpt: Option[BeaconRDD[K]]
                                          ): RDD[(K, Iterable[V])] = {

        val spooky = ec.spooky

        val preprocessed = preprocessing.getInstance[K](ec).groupByKey(rdd, beaconRDDOpt)

        val bifurcated: RDD[((Option[TraceView], Option[K]), Seq[V])] = preprocessed
          .map {
            case (k: TraceView, v) =>
              val c = k.children
              val result: (Option[TraceView], Option[K]) = {
                if (c.exists(_.isInstanceOf[UAVAction])) Some(k) -> None
                else None -> Some(k: K)
              }
              result -> v
            case (k, v) =>
              val result: (Option[TraceView], Option[K]) = None -> Some(k)
              result -> v
          }
          .mapValues(v => v.toList)

        ec.scratchRDDs.persist(bifurcated)

        val hasNavRows: Array[(TraceView, Seq[V])] = bifurcated
          .flatMap(tt => tt._1._1.map(v => v -> tt._2)).collect()

        val linkRDD = Link.availableLinkRDD(spooky)

        val allUAVs = linkRDD.keys.collect()
        val uavs = numUAVsOpt match {
          case Some(n) => allUAVs.slice(0, n)
          case None => allUAVs
        }

        val solver = JSpritSolver(JSprit.this, spooky, uavs, hasNavRows)
        val uav2RowsMap: Map[LinkStatus, Seq[(TraceView, Seq[V])]] = solver.getUAV2RowsMap

        val realignedRDD: RDD[(K, Iterable[V])] = linkRDD.flatMap {
          tuple =>
            val link = tuple._2
            val KVs = uav2RowsMap.getOrElse(tuple._1, Nil)
            val result = KVs.map {
              kv =>
                val vv = kv._1
                val updatedVV = vv.copy(children = List(PreferUAV(tuple._1)) ++ vv.children)
                (updatedVV: K) -> kv._2
            }
            result
        }

        val notHaveUAVRDD: RDD[(K, Iterable[V])] = bifurcated
          .flatMap(tt => tt._1._2.map(v => v -> tt._2))

        val result = realignedRDD.union(notHaveUAVRDD)

        result
      }

      //        val status_traceRDD: RDD[(UAVStatus, Seq[TraceView])] = spooky.sparkContext.parallelize(status_traces)
      //
      //        //TODO: cogroup is expensive
      //        //if you don't know cogroup preserve sequence, don't use it.
      //        val realignedTraceRDD: RDD[(Seq[TraceView], Link)] = linkRDD.cogroup {
      //          status_traceRDD
      //          //linkRDD doesn't have a partitioner so no need to explicit
      //        }
      //          .values
      //          .map {
      //            tuple =>
      //              assert(tuple._1.size == 1)
      //              assert(tuple._2.size == 1)
      //              tuple._2.head -> tuple._1.head
      //          }
      //
      //        val trace_index_linkRDD: RDD[(K, (Int, Link))] = realignedTraceRDD.flatMap {
      //          tuple =>
      //            tuple._1
      //              .zipWithIndex
      //              .map {
      //                tt =>
      //                  tt._1 -> (tt._2 -> tuple._2)
      //              }
      //        }
      //
      //        val cogroupedRDD: RDD[(K, (Iterable[(Int, Link)], Iterable[V]))] =
      //          trace_index_linkRDD.cogroup {
      //            rdd
      //          }
      //
      //        val kv_index = cogroupedRDD.map {
      //          triplet =>
      //            val is_links = triplet._2._1.toSeq
      //            is_links.size match {
      //              case 0 =>
      //                val k = triplet._1
      //                (k -> triplet._2._2) -> -1
      //              case 1 =>
      //                val k = triplet._1.asInstanceOf[TraceView]
      //                val updatedK = k.copy(children = List(PreferUAV(is_links.head._2)) ++ k.children)
      //                (updatedK -> triplet._2._2) -> is_links.head._1
      //              case _ =>
      //                throw new AssertionError(s"size cannot be ${is_links.size}")
      //            }
      //        }
      //
      //        kv_index.mapPartitions {
      //          itr =>
      //            itr.toSeq.sortBy(_._2).map(_._1).iterator
      //        }
      //
      //        // merge with trace that doesn't contain UAVNavigations
      //        // also carry data with them, by treating realigned link_traceRDD like a beaconRDD.
      //      }
    }
  }

  // all adaptive improvements goes here.
  object DRL extends GenPartitioner {

    def getInstance[K: ClassTag](ec: ExecutionContext): Instance[K] = {
      ???
    }
  }
}
