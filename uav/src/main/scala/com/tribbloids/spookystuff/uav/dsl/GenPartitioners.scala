package com.tribbloids.spookystuff.uav.dsl

import com.graphhopper.jsprit.core.algorithm.box.Jsprit
import com.graphhopper.jsprit.core.problem.job.Service
import com.graphhopper.jsprit.core.problem.vehicle.{VehicleImpl, VehicleTypeImpl}
import com.graphhopper.jsprit.core.problem.{Capacity, VehicleRoutingProblem, Location => JLocation}
import com.graphhopper.jsprit.core.util.{Coordinate, FastVehicleRoutingTransportCostsMatrix, Solutions}
import com.tribbloids.spookystuff.actions.TraceView
import com.tribbloids.spookystuff.dsl.GenPartitioner
import com.tribbloids.spookystuff.dsl.GenPartitioners.Instance
import com.tribbloids.spookystuff.execution.ExecutionPlan
import com.tribbloids.spookystuff.row.BeaconRDD
import com.tribbloids.spookystuff.uav.UAVConf
import com.tribbloids.spookystuff.uav.actions.{UAVAction, UAVNavigation}
import com.tribbloids.spookystuff.uav.planning.{PreferUAV, WrapLocation}
import com.tribbloids.spookystuff.uav.spatial.NED
import com.tribbloids.spookystuff.uav.telemetry.{Link, LinkStatus}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * Created by peng on 31/12/16.
  */
object GenPartitioners {

  import com.tribbloids.spookystuff.utils.SpookyViews._

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

        // get available drones
        val linkRDD = spooky.sparkContext
          .mapPerExecutorCore {
            spooky.withSession {
              session =>
                val linkTry = Link.trySelect (
                  spooky.getConf[UAVConf].uavsInFleetShuffled,
                  session
                )
                linkTry.map {
                  link =>
                    link.status() -> link
                }
            }
          }
          .flatMap(_.toOption.toSeq)
        val links = linkRDD.keys.collect()

        val traces_linkOpts_indices: Array[((TraceView, Option[LinkStatus]), Int)] = {
          val fromLinks: Array[(TraceView, Option[LinkStatus])] =
            links.map {
              link =>
                TraceView(List(WrapLocation(link.currentLocation))) -> Some(link)
            }

          val fromTraces: Array[(TraceView, Option[LinkStatus])] =
            hasUAVTraces.map {
              trace =>
                trace -> None
            }

          (fromLinks ++ fromTraces).zipWithIndex
        }
        val trace_indices: Array[(TraceView, Int)] = traces_linkOpts_indices.map {
          triplet =>
            triplet._1._1 -> triplet._2
        }

        val costEstimator = spooky.getConf[UAVConf].costEstimator
        val homeLocation = spooky.getConf[UAVConf].homeLocation

        val dMat = for (
          i <- trace_indices;
          j <- trace_indices
        ) yield {
          val traceView: TraceView = i._1
          val last = traceView.children.collect{case v: UAVNavigation => v}.last
          val lastLocation = last._to
          val cost = costEstimator.estimate(
            List(WrapLocation(lastLocation)) ++ j._1.children,
            spooky
          )
          (i._2, j._2, cost)
        }

        val jCosts = {
          val builder = FastVehicleRoutingTransportCostsMatrix.Builder
            .newInstance(trace_indices.length, false)
          dMat.foreach {
            entry =>
              builder.addTransportDistance(entry._1, entry._2, entry._3)
          }
          builder.build()
        }

        val cap = Capacity.Builder.newInstance()
          .addDimension(0, 1)
          .build()
        val jVType = VehicleTypeImpl.Builder.newInstance("UAV")
          .setCapacityDimensions(cap)
          .build()

        val jVehicles = traces_linkOpts_indices
          .flatMap {
            triplet =>
              triplet._1._2.map {v => v -> triplet._2}
          }
          .map {
            tuple =>
              val status = tuple._1
              val location = status.currentLocation
              val coord = location.getCoordinate(NED, homeLocation).get
              val jLocation = JLocation.Builder.newInstance()
                .setIndex(tuple._2)
                .setCoordinate(
                  Coordinate.newInstance(
                    coord.east,
                    coord.north
                  )
                )
                .build()
              val jVehicle = VehicleImpl.Builder
                .newInstance(status.uav.fullID)
                .setType(jVType)
                .setStartLocation(jLocation)
                .build()
              jVehicle
          }

        //TODO: why not use shipment? has better visualization.
        val jobs: Array[Service] = traces_linkOpts_indices
          .flatMap {
            triplet =>
              triplet._1._2 match {
                case Some(_) =>
                  None
                case None =>
                  Some(triplet._1._1 -> triplet._2)
              }
          }
          .map {
            tuple =>
              val navs: Seq[UAVNavigation] = tuple._1.children.collect {
                case nav: UAVNavigation => nav
              }

              val coord = navs.head._from.getCoordinate(NED, homeLocation).get
              val location = JLocation.Builder
                .newInstance()
                .setIndex(tuple._2)
                .setCoordinate(
                  Coordinate.newInstance(
                    coord.east,
                    coord.north
                  )
                )
                .build()

              Service.Builder.newInstance(tuple._1.TreeNode.treeString)
                .setLocation(location)
                .build()
          }

        val vrp = {
          val builder = VehicleRoutingProblem.Builder.newInstance()
            .setRoutingCost(jCosts)
          for (v <- jVehicles) {
            builder.addVehicle(v)
          }
          for (s <- jobs) {
            builder.addJob(s)
          }
          builder.build()
        }
        val vra = Jsprit.createAlgorithm(vrp)

        val solutions = vra.searchSolutions
        val best = Solutions.bestOf(solutions)

        import scala.collection.JavaConverters._

        val link_traces: Seq[(LinkStatus, Seq[TraceView])] = best.getRoutes.asScala.toSeq.map {
          route =>
            val link = links.find(_.uav.fullID == route.getVehicle.getId).get
            val tours = route.getTourActivities.getActivities.asScala
            val traces = for (tour <- tours) yield {
              val index = tour.getLocation.getIndex
              val trace = trace_indices.find(_._2 == index).get._1
              trace
            }
            link -> traces
        }

        val link_traceRDD: RDD[(LinkStatus, Seq[TraceView])] = spooky.sparkContext.parallelize(link_traces)

        //TODO: cogroup is expensive

        //if you don't know cogroup preserve sequence, don't use it.
        val realignedTraceRDD: RDD[(Seq[TraceView], Link)] = linkRDD.cogroup {
          link_traceRDD
          //linkRDD doesn't have a partitioner so no need to explicit
        }
          .values
          .map {
            tuple =>
              assert(tuple._1.size == 1)
              assert(tuple._2.size == 1)
              tuple._2.head -> tuple._1.head
          }

        val trace_index_linkRDD: RDD[(K, (Int, Link))] = realignedTraceRDD
          .flatMap {
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
