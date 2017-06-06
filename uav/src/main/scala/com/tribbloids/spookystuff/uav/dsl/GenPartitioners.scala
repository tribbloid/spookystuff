package com.tribbloids.spookystuff.uav.dsl

import com.graphhopper.jsprit.core.problem.vehicle.{VehicleImpl, VehicleTypeImpl}
import com.graphhopper.jsprit.core.problem.{Capacity, VehicleRoutingProblem, Location => JLocation}
import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions.{Trace, TraceView}
import com.tribbloids.spookystuff.dsl.GenPartitioner
import com.tribbloids.spookystuff.dsl.GenPartitioners.Instance
import com.tribbloids.spookystuff.execution.ExecutionPlan
import com.tribbloids.spookystuff.row.BeaconRDD
import com.tribbloids.spookystuff.uav.UAVConf
import com.tribbloids.spookystuff.uav.actions.UAVAction
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

        val uavActions: Array[Trace] = uavRDD
          .collect()

        val costEstimator = spooky.submodule[UAVConf].costEstimator

        // get available drones
        val sc = rdd.sparkContext
        val statusRDD: RDD[LinkStatus] = sc.mapPerExecutorCore {
          spooky.withSession {
            session =>
              val linkTry = Link.trySelect(
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

        val cap = Capacity.Builder.newInstance()
          .addDimension(0, 1)
          .build()
        val jVType = VehicleTypeImpl.Builder.newInstance("UAV")
          .setCapacityDimensions(cap)



        val jVehicles = statusRDD
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
