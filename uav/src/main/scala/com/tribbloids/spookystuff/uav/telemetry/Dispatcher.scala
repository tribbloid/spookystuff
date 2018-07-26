package com.tribbloids.spookystuff.uav.telemetry

import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.uav.system.UAV
import com.tribbloids.spookystuff.uav.{LinkDepletedException, UAVConf}
import com.tribbloids.spookystuff.utils.CommonUtils
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

/**
  * @return a link that fulfills ALL of the following conditions:
  * 1. NOT owned by an ongoing LifespanContext (task or thread not completed)
  * 2. NOT locked, or locked by the same mutexID (if provided)
  * 3. in uavList
  * if multiple links are available, the one with highest priority is chosen:
  *   threadLocal > prefer > others
  */
case class Dispatcher(
                       uavList: Seq[UAV],
                       session: Session,
                       mutexIDOpt: Option[Long] = None, //can unlock Link with the same mutexID
                       prefer: Seq[Link] => Option[Link] = {_.headOption},
                       recommissionWithNewProxy: Boolean = true
                     ) {

  val spooky = session.spooky
  val ctx = session.lifespan.ctx

  val conf = spooky.getConf[UAVConf]
  val factory = conf.routing

  lazy val tidOpt = Option(ctx.thread).map(_.getId)

  def getOrCreate(uav: UAV): Link = {

    val link = Link.synchronized {
      Link.registered.getOrElse (
        uav,
        {
          val factory = spooky.getConf[UAVConf].routing
          val link = factory.apply(uav)
          link.register(
            spooky,
            factory
          )
          link
        }
      )
    }

    link
  }

  def getOrCreateAll(): Unit = {
    _getAvailableOpt.foreach {
      case (uav, None) =>
        getOrCreate(uav)
      case _ =>
    }
  }

  def _getAvailableOpt: Seq[(UAV, Option[Link])] = {
    val registered: Seq[(UAV, Option[Link])] = uavList.map {
      uav =>
        uav -> Link.registered.get(uav)
    }

    val available = registered.filter {
      case v@ (_, None) => true
      case v@ (_, Some(link)) =>
        link.isAvailableTo(mutexIDOpt, Some(ctx))
    }
    available
  }

  def getAvailable: Seq[Link] = {
    getOrCreateAll()
    _getAvailableOpt.flatMap(_._2)
  }

  // ================ main API ===================

  def get: Link = {

    tryGet.get
  }

  def tryGet: Try[Link] = Try {

    CommonUtils.retry(3, 1000) {
      getOnce
    }
  }

  private def getOnce: Link = {

    var _logInfo = ArrayBuffer.empty[String]

    val chosen = Link.synchronized {
      val available = getAvailable
      val threadLocal = available.flatMap {
        v =>
          (tidOpt, v.ownerOpt.map(_.thread.getId)) match {
            case (Some(tc1), Some(tc2)) if tc1 == tc2 => Some(v)
            case _ => None
          }
      }

      require(
        threadLocal.size <= 1,
        s"""
           |Multiple Links cannot share task context or thread:
           |${threadLocal.map(_.status()).mkString("\n")}
          """.stripMargin
      )

      val preferred: Option[Link] = threadLocal.headOption
        .orElse {
          _logInfo :+= s"${ctx.toString}: ThreadLocal link not found ... choosing from other links"
          val owners = Link.registered.values.flatMap(_.ownerOpt)
          if (owners.nonEmpty)
            _logInfo :+= owners.mkString("[\n", "\n", "\n]")

          val preferred = prefer(available)
          preferred
        }

      preferred.foreach(_.owner = ctx)

      preferred
    }

    val recommissioned = chosen.map {
      v =>
        val result = if (recommissionWithNewProxy) {
          val factory = spooky.getConf[UAVConf].routing
          v.recommission(factory)
        }
        else {
          v
        }
        result.connect()

        result
    }

    LoggerFactory.getLogger(this.getClass).info(_logInfo.mkString("\n"))

    recommissioned.getOrElse {

      val msg = if (Link.registered.isEmpty) {
        s"No telemetry Link for ${uavList.mkString("[", ", ", "]")}:"
      }
      else {
        "All telemetry links are busy:"
      }
      val info = (Seq(msg) ++ Link.statusStrs).mkString("[\n","\n","\n]")
      throw new LinkDepletedException(ctx.toString + " " +info)
    }
  }
}
