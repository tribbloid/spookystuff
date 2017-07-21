package com.tribbloids.spookystuff.uav.telemetry

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.uav.UAVConf
import com.tribbloids.spookystuff.uav.system.UAV
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

object LinkUtils {

  import com.tribbloids.spookystuff.utils.SpookyViews._

  def tryLinkRDD(spooky: SpookyContext) = {

    spooky.sparkContext.mapAtLeastOncePerExecutorCore (
      {
        spooky.withSession {
          session =>
            val uavsInFleet: Seq[UAV] = spooky.getConf[UAVConf].uavsInFleetShuffled
            val linkTry = Link.Selector (
              uavsInFleet,
              session
            )
              .trySelect
            linkTry
        }
      },
      Some(spooky.sparkContext.defaultParallelism)
    )
  }

  // get available drones, TODO: merge other impl to it.
  def availableLinkRDD(spooky: SpookyContext): RDD[Link] = {

    val trial = tryLinkRDD(spooky)
    val available = trial.flatMap {
      v =>
        v.recover {
          case e =>
            LoggerFactory.getLogger(this.getClass).warn(e.toString)
            throw e
        }
          .toOption
    }
    available
  }

  def lockedLinkRDD(spooky: SpookyContext): RDD[Link] = {

    val available = availableLinkRDD(spooky)
    val locked = available.map {
      link =>
        link.lock()
        link
    }

    locked.persist()
    locked.count()

    val allUAVStatuses = locked.map(_.status()).collect()
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
    locked
  }

  def unlockAll(): Unit = {
    Link.existing.values.foreach {
      link =>
        link.unlock()
    }
  }
}
