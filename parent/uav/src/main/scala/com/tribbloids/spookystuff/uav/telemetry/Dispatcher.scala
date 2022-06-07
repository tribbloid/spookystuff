package com.tribbloids.spookystuff.uav.telemetry

import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.uav.system.UAV
import com.tribbloids.spookystuff.uav.utils.Binding
import com.tribbloids.spookystuff.uav.{LinkDepletedException, UAVConf}
import com.tribbloids.spookystuff.utils.CommonUtils
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

/**
  * @return
  *   a link that fulfills ALL of the following conditions:
  *   1. NOT owned by an ongoing LifespanContext (task or thread not completed) 2. NOT locked, or locked by the same
  *      mutexID (if provided) 3. in uavList if multiple links are available, the one with highest priority is chosen:
  *      threadLocal > prefer > others
  */
case class Dispatcher(
    uavList: List[UAV],
    session: Session,
    lock: Binding = Binding.Transient(), // Lock.ctx is ignored, session.lifespan.ctx will be used instead
    prefer: List[Link] => Option[Link] = { _.headOption },
    recommissionWithNewProxy: Boolean = true
) {

  val spooky = session.spooky
  val ctx = session.lifespan.ctx
  val _lock = lock.copy(ctx = ctx)

  val conf = spooky.getConf[UAVConf]
  val factory = conf.routing

  def getOrCreate(uav: UAV): Link = {

    val link = Link.synchronized {
      Link.registered.getOrElse(
        uav, {
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

  def _getAvailableOpt: List[(UAV, Option[Link])] = {
    val registered: List[(UAV, Option[Link])] = uavList.map { uav =>
      uav -> Link.registered.get(uav)
    }

    val available = registered.filter {
      case v @ (_, None) => true
      case v @ (_, Some(link)) =>
        link.isAvailable(Some(_lock))
    }
    available
  }

  def getAvailable: List[Link] = {
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

    val chosenOpt = Link.synchronized {
      val available = getAvailable

      val ranked = available
        .groupBy { link =>
          link.lock.getAvailability(Some(_lock))
        }
        .toList
        .sortBy(_._1)

      val strongestOpt = ranked.lastOption

      val preferred = strongestOpt.flatMap {
        case (strength, list) =>
          if (strength == 3) {
            require(
              list.size == 1,
              s"""
                 |Multiple Links cannot have shared lock:
                 |${list.map(_.status()).mkString("\n")}
          """.stripMargin
            )

            list.headOption
          } else if (strength >= 0) {
            _logInfo :+= s"${ctx.toString}: ThreadLocal link not found ... choosing from others (strength=$strength)"
            _logInfo :+= Link.statusStrs.mkString("[\n", "\n", "\n]")

            val preferred = prefer(list)
            preferred
          } else None
      }
      preferred.foreach(_.lock = _lock)
      preferred
    }

    LoggerFactory.getLogger(this.getClass).info(_logInfo.mkString("\n"))

    val chosen = chosenOpt.getOrElse {

      val msg = if (Link.registered.isEmpty) {
        s"No Link for ${uavList.mkString("[", ", ", "]")}:"
      } else {
        s"All Links are not accessible (lock = ${_lock}):"
      }
      val info = msg + "\n" + Link.statusStrs.mkString("[\n", "\n", "\n]")
      throw new LinkDepletedException(ctx.toString + " " + info)
    }

    val recommissioned = {
      val result = if (recommissionWithNewProxy) {
        val factory = spooky.getConf[UAVConf].routing
        chosen.recommission(factory)
      } else {
        chosen
      }
      result.connect()

      result
    }

    recommissioned
  }
}
