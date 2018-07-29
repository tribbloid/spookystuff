package com.tribbloids.spookystuff.uav.telemetry

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.uav.UAVConf
import com.tribbloids.spookystuff.uav.system.UAV
import com.tribbloids.spookystuff.uav.utils.Lock
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

import scala.util.{Failure, Try}

//TODO: try not using any of these in non-testing code, they ar
object LinkUtils {

  import com.tribbloids.spookystuff.utils.SpookyViews._

  def tryLinkRDD(
                  spooky: SpookyContext,
                  onHold: Boolean = true
                ): RDD[Try[Link]] = {

    val uuidSeed = spooky.sparkContext.uuidSeed()

    val locked = uuidSeed.mapOncePerCore {
      case (i, uuid) =>
        spooky.withSession {
          session =>
            val fleet: Seq[UAV] = spooky.getConf[UAVConf].uavsInFleetShuffled
            val lock = if (onHold) Lock.OnHold(Some(uuid))
            else Lock.Transient(Some(uuid))
            val linkTry = Dispatcher (
              fleet,
              session,
              lock
            )
              .tryGet
            linkTry
        }
    }

    //    locked.persist()
    //    locked.count()
    val result = locked

    result
  }

  def linkRDD(
               spooky: SpookyContext,
               onHold: Boolean = true
             ): RDD[Link] = {

    val proto = tryLinkRDD(spooky, onHold)
    val result = proto.flatMap {
      v =>
        v
          .recoverWith {
            case e =>
              LoggerFactory.getLogger(this.getClass).warn(e.toString)
              Failure(e)
          }
          .toOption
    }

    result
  }

  def unlockAll(): Unit = {// TODO: made distributed
    Link.registered.values.foreach {
      link =>
        link.unlock()
    }
  }
}
