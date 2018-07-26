package com.tribbloids.spookystuff.uav.telemetry

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.uav.UAVConf
import com.tribbloids.spookystuff.uav.system.UAV
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

import scala.util.{Failure, Try}

//TODO: try not using any of these in non-testing code, they ar
object LinkUtils {

  import com.tribbloids.spookystuff.utils.SpookyViews._

  def tryLinkRDD(
                  spooky: SpookyContext,
                  lock: Boolean = true
                ): RDD[Try[Link]] = {

    val locked = spooky.sparkContext.mapAtLeastOncePerExecutorCore (
      {
        spooky.withSession {
          session =>
            val fleet: Seq[UAV] = spooky.getConf[UAVConf].uavsInFleetShuffled
            val linkTry = Dispatcher (
              fleet,
              session
            )
              .tryGet
            if (lock) linkTry.foreach {
              v =>
                v.lock
            }
            linkTry
        }
      },
      Some(spooky.sparkContext.defaultParallelism)
    )

//    locked.persist()
//    locked.count()
    val result = locked

    result
  }

  def linkRDD(
               spooky: SpookyContext,
               lock: Boolean = true
             ): RDD[Link] = {

    val proto = tryLinkRDD(spooky, lock)
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
