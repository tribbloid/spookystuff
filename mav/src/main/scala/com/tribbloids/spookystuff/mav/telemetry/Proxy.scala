package com.tribbloids.spookystuff.mav.telemetry

import com.tribbloids.spookystuff.caching
import com.tribbloids.spookystuff.mav.MAVConf
import com.tribbloids.spookystuff.session.python.{CaseInstanceRef, PyBinding, PythonDriver}
import com.tribbloids.spookystuff.session.{Lifespan, LocalCleanable}

object Proxy {

  val existing: caching.ConcurrentSet[Proxy] = caching.ConcurrentSet()

  //  def existingPrimaryOuts = existing.map(_.primaryOut)

  //TODO: this is currently useless:
  // 2 MAVProxy can output to the same primary port without triggering any error so there is no way to detect conflict
  // the only thing that will happen is command being replicated to 2 drones and cause an air collision :-<
  // val blacklist: caching.ConcurrentSet[String] = caching.ConcurrentSet()

  /**
    * this is a singleton daemon that lives until worker JVM dies or explicitly terminated, it centralize controls of all proxies.
    * Spark PythonWorkerFactory says spawning child process is faster using Python?
    * If doesn't work please degrade to JVM based process spawning.
    */
  private var _managerDriverOpt: Option[PythonDriver] = None

  def managerDriver = _managerDriverOpt
    .filterNot(_.isCleaned)
    .getOrElse {
      val v = new PythonDriver(lifespan = Lifespan.JVM(nameOpt = Some("ProxyManager")))
      _managerDriverOpt = Some(v)
      v
    }
}

/**
  * MAVProxy: https://github.com/ArduPilot/MAVProxy
  * outlives any python driver
  * not to be confused with dsl.WebProxy
  * CAUTION: each MAVProxy instance contains 2 python processes, keep that in mind when debugging
  */
//TODO: MAVProxy supports multiple master for multiple telemetry backup
case class Proxy(
                  master: String,
                  outs: Seq[String], //first member is always used by DK.
                  baudRate: Int,
                  ssid: Int = MAVConf.PROXY_SSID,
                  name: String
                ) extends CaseInstanceRef with LocalCleanable {

  //  this.synchronized {

  //  val existing = Proxy.existing // remember to clean up the old one to create a new one

  {
    val condition = !Proxy.existing.exists(_.master == this.master)
    assert(condition, s"master ${this.master} is already used")
  }

  //    assert(!existing.map(_.primaryOut).toSet.contains(this.primaryOut),
  //      s"primaryOut ${this.primaryOut} is already used")

  Proxy.existing += this
  //  }

  lazy val managerPy: PyBinding = this._Py(Proxy.managerDriver)

  //  override def _Py(driver: PythonDriver, spookyOpt: Option[SpookyContext]): PyBinding = {
  //    throw new UnsupportedOperationException("NOT ALLOWED! use mgrPy instead")
  //  }

  override protected def cleanImpl(): Unit = {
    super.cleanImpl()
    Proxy.existing -= this
  }
}