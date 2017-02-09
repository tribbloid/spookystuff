package com.tribbloids.spookystuff.uav.telemetry.mavlink

import com.tribbloids.spookystuff.uav.UAVConf
import com.tribbloids.spookystuff.session.ResourceLedger
import com.tribbloids.spookystuff.session.python._

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
                  ssid: Int = UAVConf.PROXY_SSID,
                  name: String
                ) extends CaseInstanceRef with BijectoryRef with ResourceLedger {

  assert(!outs.contains(master))
  override lazy val resourceIDs = Map(
    "master" -> Set(master),
    "firstOut" -> outs.headOption.toSet //need at least 1 out for executor
  )

//  {
//    val existing = Cleanable.getTyped[Proxy].filterNot(_ == this)
//    val condition = !existing.exists(_.master == this.master)
//    assert(condition, s"master ${this.master} is already used")
//  }

//  Proxy.existing += this

//  override protected def cleanImpl(): Unit = {
//    super.cleanImpl()
////    Proxy.existing -= this
//  }
}