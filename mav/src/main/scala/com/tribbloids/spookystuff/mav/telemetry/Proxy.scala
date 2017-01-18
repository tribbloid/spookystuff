package com.tribbloids.spookystuff.mav.telemetry

import com.tribbloids.spookystuff.caching
import com.tribbloids.spookystuff.mav.MAVConf
import com.tribbloids.spookystuff.session.python.{CaseInstanceRef, PyBinding, PythonDriver, SingletonRef}
import com.tribbloids.spookystuff.session.{Lifespan, LocalCleanable, ResourceLock}

object Proxy {

  val existing: caching.ConcurrentSet[Proxy] = caching.ConcurrentSet()
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
                ) extends CaseInstanceRef with SingletonRef with LocalCleanable with ResourceLock {

  assert(!outs.contains(master))
  override lazy val resourceIDs = Map(
    "master" -> Set(master),
    "firstOut" -> outs.headOption.toSet //need at least 1 out for executor
  )

  {
    val condition = !Proxy.existing.exists(_.master == this.master)
    assert(condition, s"master ${this.master} is already used")
  }

  Proxy.existing += this

  val driver = {
    val v = new PythonDriver(lifespan = Lifespan.JVM(nameOpt = Some("Proxy")))
    v
  }

  override def PY: PyBinding = this._Py(driver)

  //  override def _Py(driver: PythonDriver, spookyOpt: Option[SpookyContext]): PyBinding = {
  //    throw new UnsupportedOperationException("NOT ALLOWED! use mgrPy instead")
  //  }

  override protected def cleanImpl(): Unit = {
    super.cleanImpl()
    driver.clean()
    Proxy.existing -= this
  }
}