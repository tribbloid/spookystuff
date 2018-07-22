package com.tribbloids.spookystuff.uav.telemetry

import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.uav.system.UAV
import com.tribbloids.spookystuff.uav.{LinkDepletedException, UAVConf}
import com.tribbloids.spookystuff.utils.CommonUtils
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

case class Dispatcher(
                       fleet: Seq[UAV],
                       session: Session,
                       mutexIDOpt: Option[Long] = None, //can unlock Link with the same mutexID
                       prefer: Seq[Link] => Option[Link] = {_.headOption},
                       recommissionWithNewProxy: Boolean = true
                     ) {

  lazy val spooky = session.spooky
  lazy val ctx = session.lifespan.ctx

  def get: Link = {

    tryGet.get
  }

  def tryGet: Try[Link] = Try {

    CommonUtils.retry(3, 1000) {
      tryGetOnce
        .recover{
          case e: Throwable =>
            throw e
        }
        .get
    }
  }

  /**
    * @return a link that is unlocked and owned by session's lifespan
    */
  private def tryGetOnce: Try[Link] = {

    val tidOpt = Option(ctx.thread).map(_.getId)

    var _logInfo = ArrayBuffer.empty[String]

    val threadLocalOpt = {

      val links = Link.registered.values.toList
      val owned = links.filter {
        v =>
          val ownerTIDOpt = v.ownerOpt.map(_.thread.getId)
          val matched = (ownerTIDOpt, tidOpt) match {
            case (Some(tc1), Some(tc2)) if tc1 == tc2 => true
            case _ => false
          }
          matched
      }

      require(
        owned.size <= 1,
        s"""
           |Multiple Links cannot share task context or thread:
           |${owned.map(_.status()).mkString("\n")}
          """.stripMargin
      )
      val result = owned.find {
        link =>
          fleet.contains(link.uav)
      }
      result.foreach{_.setOwnerAndUnlock(ctx)}
      result
    }

    val recommissionedOpt = threadLocalOpt match {
      case None =>
        _logInfo :+= s"${ctx.toString}: ThreadLocal link unavailable"
        val ownership = Link.registered.values.flatMap(_.ownerOpt)
        if (ownership.nonEmpty)
          _logInfo :+= ownership.mkString("[\n","\n","\n]")
        None
      case Some(threadLocal) =>
        val v = if (recommissionWithNewProxy) {
          val factory = spooky.getConf[UAVConf].linkFactory
          threadLocal.recommission(factory)
        }
        else {
          threadLocal
        }
        v.connect()
        assert(v.uav == threadLocal.uav)
        Some(v)
    }

    // no need to recommission if the link is free
    val resultOpt = recommissionedOpt
      .orElse {
        val links = fleet.flatMap{
          uav =>
            val vv = uav.getLink(spooky)
            Try {
              vv.connect()
              vv
            }
              .toOption
        }

        val preferred = Link.synchronized {
          val available = links.filter(_.isAvailableTo(mutexIDOpt))
          val preferred = prefer(available)
          //deliberately set inside synched block to avoid being selected by 2 threads
          preferred.foreach{_.setOwnerAndUnlock(ctx)}
          preferred
        }

        preferred.foreach {
          v =>
            _logInfo :+= s"Link ${v.statusStr} is selected"
        }

        preferred
      }

    LoggerFactory.getLogger(this.getClass).info(_logInfo.mkString("\n"))

    resultOpt match {
      case Some(link) =>
        assert(
          link.owner == ctx,
          s"owner inconsistent! ${link.owner} != $ctx"
        )
        Success {
          link
        }
      case None =>
        val msg = if (Link.registered.isEmpty) {
          s"No telemetry Link for ${fleet.mkString("[", ", ", "]")}:"
        }
        else {
          "All telemetry links are busy:"
        }
        val info = (Seq(msg) ++ Link.statusStrs).mkString("[\n","\n","\n]")
        Failure(
          new LinkDepletedException(ctx.toString + " " +info)
        )
    }
  }
}

//object Selector {
//
//  def withMutex(
//                 fleet: Seq[UAV],
//                 session: Session,
//                 mutexIDOpt: Option[Long]
//               ) = Selector(
//    fleet,
//    session,
//    prefer = {
//      vs =>
//        vs.find {
//          v =>
//            mutexIDOpt.contains(v._mutexLock._id)
//        }
//    }
//  )
//}