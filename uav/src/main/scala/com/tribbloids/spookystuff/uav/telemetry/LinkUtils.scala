package com.tribbloids.spookystuff.uav.telemetry

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.uav.UAVConf
import com.tribbloids.spookystuff.uav.system.UAV
import com.tribbloids.spookystuff.uav.utils.Binding
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.util.Try

//TODO: try not using any of these in non-testing code, Spark tasks should be location agnostic
object LinkUtils {

  import com.tribbloids.spookystuff.utils.SpookyViews._

  def tryLinkRDD(
      spooky: SpookyContext,
      parallelismOpt: Option[Int] = None,
      onHold: Boolean = true
  ): RDD[Try[Link]] = {

    val uuidSeed = spooky.sparkContext.uuidSeed(
      parallelismOpt,
      debuggingInfo = Some("Dispatching Link(s)")
    )

    val result = uuidSeed.map {
      case (i, uuid) =>
        spooky.withSession { session =>
          val fleet: List[UAV] = spooky.getConf[UAVConf].uavsInFleetShuffled
          val lock =
            if (onHold) Binding.OnHold(Some(uuid))
            else Binding.Transient(Some(uuid))
          val linkTry = Dispatcher(
            fleet,
            session,
            lock
          ).tryGet
          linkTry
        }
    }

    result
  }

//  @deprecated
//  def linkRDD(
//               spooky: SpookyContext,
//               onHold: Boolean = true
//             ): RDD[Link] = {
//
//    val proto = tryLinkRDD(spooky, onHold)
//    val result = proto.flatMap {
//      v =>
//        v
//          .recoverWith {
//            case e =>
//              LoggerFactory.getLogger(this.getClass).warn(e.toString)
//              Failure(e)
//          }
//          .toOption
//    }
//
//    result
//  }

  def unlockAll(sc: SparkContext): Unit = {
    sc.runEverywhere() { _ =>
      Link.registered.values.foreach { link =>
        link.unlock()
      }
    }
  }

  def cleanAll(sc: SparkContext): Unit = {
    sc.runEverywhere() { _ =>
      Link.registered.values.toList
        .foreach(_.clean())
    }
  }
}
