package com.tribbloids.spookystuff.mav.telemetry

import com.tribbloids.spookystuff.mav.MAVConf
import com.tribbloids.spookystuff.session.{AbstractSession, Session}
import com.tribbloids.spookystuff.session.python.{CaseInstanceRef, PythonDriver, StaticRef}
import com.tribbloids.spookystuff.utils.SpookyUtils
import com.tribbloids.spookystuff.{SpookyContext, caching}
import org.slf4j.LoggerFactory

import scala.collection.Map

case class Endpoint(
                     // remember, one drone can have several telemetry
                     // endpoints: 1 primary and several backups (e.g. text message-based)
                     // TODO: implement telemetry backup mechanism, can use MAVproxy's multiple master feature
                     connStrs: Seq[String],
                     vehicleTypeOpt: Option[String] = None
                   ) extends CaseInstanceRef {

  def connStr = connStrs.head
}

object Link extends StaticRef {

  // max 1 per task/thread.
  val driverLocal: caching.ConcurrentMap[PythonDriver, Link] = caching.ConcurrentMap()

  // connStr -> (link, isBusy)
  // only 1 allowed per connStr, how to enforce?
  val existing: caching.ConcurrentMap[String, (ProxyFactory, Link)] = caching.ConcurrentMap()

  // won't be used to create any link before its status being recovered by ping daemon.
  val blacklist: caching.ConcurrentSet[String] = caching.ConcurrentSet()

  //in the air but unused
  def idle: Map[String, (ProxyFactory, Link)] = existing.filter {
    tuple =>
      !blacklist.contains(tuple._1) &&
        !tuple._2._2.isBusy
  }

  /**
    * create a telemetry link based on the following order:
    * if one is already created in the same task, reuse it
    * if one is created in a previous task and not busy, use it. The busy status is controlled by whether it has an active python driver.
    *   - if its generated by an obsolete ProxyFactory, terminate the link and immediately recreate a new one with the new ProxyFactory,
    *     being created means the drone is already in the air, and can be deployed much faster
    * * if multiple are created by previous tasks and not busy, use the one that is closest to the first waypoint * (not implemented yet)
    * If none of the above exists, create one from candidates from scratch
    * remember: once the link is created its proxy is bind to it until death.
    */
  def getOrCreate(
                   candidates: Seq[Endpoint],
                   proxyFactory: ProxyFactory,
                   session: Session
                 ): Link = {

    val driver = session.getOrProvisionPythonDriver
    driverLocal
      .get(driver)
      .orElse {
        getOrRefitIdle(candidates, proxyFactory)(Some(session.spooky))
      }
      .getOrElse {
        LoggerFactory.getLogger(this.getClass).info({
          if (existing.isEmpty) "No existing telemetry Link, creating new one"
          else "All existing telemetry Link(s) are busy, creating new one"
        })
        val neo = create(candidates, proxyFactory)(Some(session.spooky))
        neo
      }
  }

  // CAUTION: this will refit the telemetry link if ProxyFactory is different.
  def getOrRefitIdle(candidates: Seq[Endpoint], proxyFactory: ProxyFactory)(
    implicit spookyOpt: Option[SpookyContext] = None
  ): Option[Link] = {
    //TODO: change to collectFirst()?
    val idleEndpointOpt = candidates.find {
      endpoint =>
        idle.get(endpoint.connStr).nonEmpty
    }
    val idleOpt = idleEndpointOpt.flatMap {
      endpoint =>
        idle.get(endpoint.connStr)
    }

    idleOpt.map {
      tuple =>
        val existingLink = tuple._2
        if (!proxyFactory.canCreate(existingLink)) {
          existingLink.tryClean()
          val endpoint = idleEndpointOpt.get
          val result = fromFactory(endpoint, proxyFactory)
          result
        }
        else {
          existingLink
        }
    }
  }

  def create(candidates: Seq[Endpoint], proxyFactory: ProxyFactory)(
    implicit spookyOpt: Option[SpookyContext] = None
  ): Link = {
    val endpoint = candidates.find {
      v =>
        !existing.contains(v.connStr) &&
          !blacklist.contains(v.connStr)
    }
      .getOrElse(
        throw new UnsupportedOperationException(
          "Reinforcement depleted :-<\n" +
            candidates.map {
              candidate =>
                if (blacklist.contains(candidate.connStr)) s"\t${candidate.connStr} is unreachable"
                else s"\t${candidate.connStr} is busy"
            }
              .mkString("\n")
        )
      )

    fromFactory(endpoint, proxyFactory)
  }

  def fromFactory(
                   endpoint: Endpoint,
                   proxyFactory: ProxyFactory
                 )(
                   implicit spookyOpt: Option[SpookyContext] = None
                 ): Link = {

    val result = Link(
      endpoint,
      proxyFactory.apply(endpoint)
    )
    //    driverLocals.put(taskThreadInfo, result)
    existing.put(result.endpoint.connStr, proxyFactory -> result)
    result
  }

  // only set up to avoid malicious ser/de copy, in which a deep copy of Link is created with identical PID that will be killed twice by AutoCleaner
  //  val allProxyPIDs: caching.ConcurrentSet[Int] = caching.ConcurrentSet()

  final val retries = 3
}

/**
to keep a drone in the air, a python daemon process D has to be constantly running to
supervise task-irrelevant path planning (e.g. RTL/Position Hold/Avoidance).
This process outlives each task. Who launches D? how to ensure smooth transitioning
of control during Partition1 => D => Partition2 ? Can they share the same
Connection / Endpoint / Proxy ? Do you have to make them picklable ?

GCS:UDP:xxx ------------------------> Proxy:TCP:xxx -> Drone
                                   /
TaskProcess -> Connection:UDP:xx -/
            /
DaemonProcess   (can this be delayed to be implemented later? completely surrender control to GCS after Altitude Hold)
  is Vehicle picklable? if yes then that changes a lot of things.
  but if not ...
    how to ensure that an interpreter can takeover and get the same vehicle?
  */
case class Link private[telemetry](
                                    endpoint: Endpoint,
                                    proxyOpt: Option[Proxy]
                                  )(
                                    implicit val spookyOpt: Option[SpookyContext] = None
                                  ) extends CaseInstanceRef {

  spookyOpt.foreach {
    spooky =>
      spooky.metrics.linkCreated += 1
  }

  // TODO: why synchronized? remove it if too slow
  override def Py(session: AbstractSession): PyBinding = this.synchronized {
    val py = super.Py(session)
    try {
      assert(
        this.driverToBindings.size <= 1,
        "Another Python process is still alive, close that first to avoid port conflict"
      )

      val driver = session.pythonDriver
      if (!Link.driverLocal.contains(driver)) {
        Link.driverLocal.put(driver, this)
        SessionView(session).start()
      }

      py
    }
    catch {
      case e: Throwable =>
        py.tryClean()
        throw e
    }
  }

  case class SessionView(session: AbstractSession) {

    val binding = Py(session)

    // will retry 6 times, try twice for Vehicle.connect() in python, if failed, will restart proxy and try again (3 times).
    // after all attempts failed will add endpoint into blacklist.
    def start(): String = {

      try {
        var needRestart = false
        val retries = session.spooky.conf.components.get[MAVConf]().connectionRetries
        SpookyUtils.retry(retries) {
          proxyOpt.foreach {
            proxy =>
              if (needRestart) {
                proxy.mgrPy.restart()
              }
              else {
                proxy.mgrPy.start()
                needRestart = true
              }
          }
          val result = binding.start().strOpt.get
          result
        }
      }
      catch {
        case e: Throwable =>
          LoggerFactory.getLogger(this.getClass).error(s"${endpoint.connStr} is unreachable, adding to blacklist")

          //TODO: enable after ping daemon is implemented
          //            Link.blacklist += endpoint.connStr
          throw e
      }
    }
  }

  override def cleanImpl(): Unit = {

    proxyOpt.foreach(_.tryClean())
    //    proxyPIDOpt
    //      .filter(Link.allProxyPIDs.contains)
    //      .foreach {
    //        pid =>
    //          this.scratchDriver {
    //            driver =>
    //              Link._Py(driver).killProxy(pid)
    //          }
    //          Link.allProxyPIDs -= pid
    //      }
    super.cleanImpl()
    Link.existing -= this.endpoint.connStr
  }

  override def bindingCleaningHook(pyBinding: PyBinding): Unit = {
    pyBinding.spookyOpt.foreach {
      spooky =>
      //        spooky.metrics.linkDestroyed += 1
    }
  }

  // return true if its binding's python driver is not dead
  def isBusy: Boolean = {
    this.bindings.nonEmpty
  }
}