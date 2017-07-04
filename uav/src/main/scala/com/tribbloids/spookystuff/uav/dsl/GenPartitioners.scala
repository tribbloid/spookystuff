package com.tribbloids.spookystuff.uav.dsl

import com.graphhopper.jsprit.core.problem.solution.VehicleRoutingProblemSolution
import com.tribbloids.spookystuff.actions.TraceView
import com.tribbloids.spookystuff.dsl.GenPartitioner
import com.tribbloids.spookystuff.dsl.GenPartitioners.Instance
import com.tribbloids.spookystuff.execution.ExecutionPlan
import com.tribbloids.spookystuff.row.BeaconRDD
import com.tribbloids.spookystuff.uav.actions.UAVAction
import com.tribbloids.spookystuff.uav.planning.{JSpritSolver, PreferUAV, WrapLocation}
import com.tribbloids.spookystuff.uav.telemetry.{Link, UAVStatus}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * Created by peng on 31/12/16.
  */
object GenPartitioners {

  case class JSprit(
                     vizPathOpt: Option[String] = Some("output/GP.png") // for debugging only.
                   ) extends GenPartitioner {

    def getInstance[K >: TraceView: ClassTag](ec: ExecutionPlan.Context): Instance[K] = {
      Inst[K](ec)
    }

    case class Inst[K >: TraceView](ec: ExecutionPlan.Context)(
      implicit val ctg: ClassTag[K]
    ) extends Instance[K] {

      //gather all UAVActions to driver and use a local solver (JSprit) to rearrange them.
      override def groupByKey[V: ClassTag](
                                            rdd: RDD[(K, V)],
                                            beaconRDDOpt: Option[BeaconRDD[K]]
                                          ): RDD[(K, Iterable[V])] = {

        val spooky = ec.spooky

        ec.scratchRDDs.persist(rdd)

        val hasUAVRDD: RDD[TraceView] = rdd.flatMap {
          case (k: TraceView, _) =>
            val c = k.children
            if (c.exists(_.isInstanceOf[UAVAction])) Some(k)
            else None
          case _ =>
            None
        }
          .distinct()

        //        val hasUAVTraces_Data: Array[(TraceView, V)] = hasUAVRDD.distinct().collect()
        //        val hasUAVTraces = hasUAVTraces_Data.map(_._1.children)

        val hasUAVTraces: Array[TraceView] = hasUAVRDD.collect()

        val linkRDD = Link.linkRDD(spooky)
        val links = linkRDD.keys.collect()

        val traces_linkOpts_indices: Array[((TraceView, Option[UAVStatus]), Int)] = {
          val fromLinks: Array[(TraceView, Option[UAVStatus])] =
            links.map {
              link =>
                TraceView(List(WrapLocation(link.currentLocation))) -> Some(link)
            }

          val fromTraces: Array[(TraceView, Option[UAVStatus])] =
            hasUAVTraces.map {
              trace =>
                trace -> None
            }

          (fromLinks ++ fromTraces).zipWithIndex
        }

        val best: VehicleRoutingProblemSolution = JSpritSolver.vrpSolution(spooky, traces_linkOpts_indices)

        import scala.collection.JavaConverters._

        val status_traces: Seq[(UAVStatus, Seq[TraceView])] = best.getRoutes.asScala.toList.map {
          route =>
            val link = links.find(_.uav.fullID == route.getVehicle.getId).get
            val tours = route.getTourActivities.getActivities.asScala
            val traces = for (tour <- tours) yield {
              val index = tour.getLocation.getIndex
              val trace = traces_linkOpts_indices.find(_._2 == index).get._1._1
              trace
            }
            link -> traces
        }

        val status_traceRDD: RDD[(UAVStatus, Seq[TraceView])] = spooky.sparkContext.parallelize(status_traces)

        //TODO: cogroup is expensive
        //if you don't know cogroup preserve sequence, don't use it.
        val realignedTraceRDD: RDD[(Seq[TraceView], Link)] = linkRDD.cogroup {
          status_traceRDD
          //linkRDD doesn't have a partitioner so no need to explicit
        }
          .values
          .map {
            tuple =>
              assert(tuple._1.size == 1)
              assert(tuple._2.size == 1)
              tuple._2.head -> tuple._1.head
          }

        val trace_index_linkRDD: RDD[(K, (Int, Link))] = realignedTraceRDD.flatMap {
          tuple =>
            tuple._1
              .zipWithIndex
              .map {
                tt =>
                  tt._1 -> (tt._2 -> tuple._2)
              }
        }

        val cogroupedRDD: RDD[(K, (Iterable[(Int, Link)], Iterable[V]))] =
          trace_index_linkRDD.cogroup {
            rdd
          }

        val kv_index = cogroupedRDD.map {
          triplet =>
            val is_links = triplet._2._1.toSeq
            is_links.size match {
              case 0 =>
                val k = triplet._1
                (k -> triplet._2._2) -> -1
              case 1 =>
                val k = triplet._1.asInstanceOf[TraceView]
                val updatedK = k.copy(children = List(PreferUAV(is_links.head._2)) ++ k.children)
                (updatedK -> triplet._2._2) -> is_links.head._1
              case _ =>
                throw new AssertionError(s"size cannot be ${is_links.size}")
            }
        }

        kv_index.mapPartitions {
          itr =>
            itr.toSeq.sortBy(_._2).map(_._1).iterator
        }

        // merge with trace that doesn't contain UAVNavigations
        // also carry data with them, by treating realigned link_traceRDD like a beaconRDD.
      }
    }
  }

  // all adaptive improvements goes here.
  object DRL extends GenPartitioner {

    def getInstance[K: ClassTag](ec: ExecutionPlan.Context): Instance[K] = {
      ???
    }
  }
}
