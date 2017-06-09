package com.tribbloids.spookystuff.uav.dsl

import com.graphhopper.jsprit.core.problem.vehicle.{VehicleImpl, VehicleTypeImpl}
import com.graphhopper.jsprit.core.problem.{Capacity, VehicleRoutingProblem, Location => JLocation}
import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions.{Action, Trace, TraceView}
import com.tribbloids.spookystuff.dsl.GenPartitioner
import com.tribbloids.spookystuff.dsl.GenPartitioners.Instance
import com.tribbloids.spookystuff.execution.ExecutionPlan
import com.tribbloids.spookystuff.row.BeaconRDD
import com.tribbloids.spookystuff.uav.UAVConf
import com.tribbloids.spookystuff.uav.actions.{UAVAction, UAVNavigation}
import com.tribbloids.spookystuff.uav.planning.FromLocation
import com.tribbloids.spookystuff.uav.telemetry.{Link, LinkStatus}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * Created by peng on 31/12/16.
  */
object GenPartitioners {

  import com.tribbloids.spookystuff.utils.SpookyViews._

  case class JSprit(
                     backup: GenPartitioner // for any key that doesn't contain UAVAction.
                   ) extends GenPartitioner {

    def getInstance[K: ClassTag](ec: ExecutionPlan.Context): Instance[K] = {
      Inst(ec)
    }

    case class Inst[K](ec: ExecutionPlan.Context)(
      implicit val ctg: ClassTag[K]
    ) extends Instance[K] {

      lazy val backupInstance = backup.getInstance[K](ec)

      //gather all UAVActions to driver and use a local solver (JSprit) to rearrange them.
      override def groupByKey[V: ClassTag](
                                            rdd: RDD[(K, V)],
                                            beaconRDDOpt: Option[BeaconRDD[K]]
                                          ): RDD[(K, Iterable[V])] = {

        val proto: RDD[(K, Iterable[V])] =
          backupInstance.groupByKey(rdd, beaconRDDOpt)

        val spooky = ec.spooky

        //        val bifurcated = proto.map {
        //          case (k: TraceView, v) =>
        //            if (k.children.exists(_.isInstanceOf[UAVAction])) {
        //              Some(k -> v) -> None
        //            }
        //            else {
        //              None -> Some(k -> v)
        //            }
        //          case tt@ _ =>
        //            None -> Some(tt)
        //        }
        //
        //        ec.scratchRDDs.persist(bifurcated)
        //
        //        val uavRDD = bifurcated.flatMap(_._1)
        //        val notUAVRDD = bifurcated.flatMap(_._2)

        val uavRDD: RDD[Trace] = proto.flatMap {
          case (k: TraceView, v)
            if k.children.exists(_.isInstanceOf[UAVAction]) =>
            Some(k.children)
          case _ =>
            None
        }
        val costEstimator = spooky.submodule[UAVConf].costEstimator

        val uavTraces: Array[Trace] = uavRDD.collect()

        // get available drones
        val linkRDD: RDD[LinkStatus] = spooky.sparkContext
          .mapPerExecutorCore {
            spooky.withSession {
              session =>
                val linkTry = Link.trySelect (
                  spooky.submodule[UAVConf].uavsInFleetShuffled,
                  session
                )
                linkTry.map {
                  link =>
                    link.status()
                }
            }
          }
          .flatMap(_.toOption.toSeq)
        val links = linkRDD.collect()

        val traces_linkOpts_ids = links.map {
          link =>
            List(FromLocation(link.currentLocation)) -> Some(link)
        } ++ uavTraces.map {
          trace =>
            trace -> None
        }
          .zipWithIndex
        val trace_ids = traces_linkOpts_ids.map {
          triplet =>
            triplet
        }

        val dMat = for (
          i <- trace_ids;
          j <- trace_ids
        ) {
          val last = i._1.collect{case v: UAVNavigation => v}.last
          val lastLocation = last._to
          val cost = spooky.submodule[UAVConf].costEstimator.estimate(
            List(FromLocation(lastLocation)) ++ j._1,
            spooky
          )
          (i._2, j._2, cost)
        }

        val cap = Capacity.Builder.newInstance()
          .addDimension(0, 1)
          .build()
        val jVType = VehicleTypeImpl.Builder.newInstance("UAV")
          .setCapacityDimensions(cap)

        val jVehicles = linkRDD
          .collect()
          .map {
            status =>
              val location = status.currentLocation
              val jLocation = JLocation.Builder
                .newInstance()
                .setIndex()
              val jVehicle = VehicleImpl.Builder
                .newInstance(status.uav.fullName)
                .setStartLocation(status.currentLocation)
                .build()
              jVehicle
          }
        val jJobs =

        val vrpBuilder =  VehicleRoutingProblem.Builder.newInstance()
        vrpBuilder.addJob(service).addJob(shipment).addVehicle(vehicle1).addVehicle(vehicle2));
        vrpBuilder.setFleetSize(FleetSize.FINITE);
        VehicleRoutingProblem problem =  vrpBuilder.build();

      }
    }
  }

  // all adaptive improvements goes here.
  object DRL extends GenPartitioner {

    override def getInstance[K: ClassTag](spooky: SpookyContext) = ???
  }
}
