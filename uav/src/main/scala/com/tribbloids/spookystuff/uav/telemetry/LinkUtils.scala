package com.tribbloids.spookystuff.uav.telemetry

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.uav.UAVConf
import com.tribbloids.spookystuff.uav.system.UAV
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

import scala.util.Try

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
            linkTry.foreach {
              v =>
                v.lock()
            }
            linkTry
        }
      },
      Some(spooky.sparkContext.defaultParallelism)
    )

    locked.persist()

    val allUAVStatuses = locked.flatMap(v => v.toOption.map(_.status())).collect()
    val uri_statuses = allUAVStatuses.flatMap {
      status =>
        status.uav.uris.map {
          uri =>
            uri -> status
        }
    }
    val grouped = uri_statuses.groupBy(_._1)
    grouped.values.foreach {
      v =>
        assert(
          v.length == 1,
          s""""
             |multiple UAVs sharing the same uris:
             |${v.map {
            vv =>
              s"${vv._2.uav} @ ${vv._2.ownerOpt.getOrElse("[MISSING]")}"
          }
            .mkString("\n")}
             """.stripMargin
        )
    }

    val result = if (!lock) {
      locked.map {
        v =>
          v.map(_.unlock())
          v
      }
    }
    else {
      locked
    }

    result
  }

  def linkRDD(
               spooky: SpookyContext,
               lock: Boolean = true
             ): RDD[Link] = {

    val proto = tryLinkRDD(spooky, lock)
    val result = proto.flatMap {
      v =>
        v.recover {
          case e =>
            LoggerFactory.getLogger(this.getClass).warn(e.toString)
            throw e
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
