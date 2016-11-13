package com.tribbloids.spookystuff.mav.telemetry


import com.tribbloids.spookystuff.caching
import com.tribbloids.spookystuff.session.Cleanable
import com.tribbloids.spookystuff.session.python.CaseInstanceRef

object Proxy {

  val usedPorts: caching.ConcurrentSet[Int] = caching.ConcurrentSet()

  def apply(
             master: String,
             port: Int,
             gcsOuts: Seq[String]
           ): Proxy = {

    val outs = Seq(s"udp:localhost:$port") ++ gcsOuts
    val result = Proxy(
      master,
      outs,
      "DRONE@" + master
    )
    Proxy.usedPorts += port
    result
  }
}

/**
  * MAVProxy: https://github.com/ArduPilot/MAVProxy
  * outlives any python driver
  * not to be confused with dsl.WebProxy
  * CAUTION: each MAVProxy instance contains 2 python processes, keep that in mind when debugging
  */
case class Proxy(
                  master: String,
                  outs: Seq[String], //first member is always used by DK.
                  name: String
                ) extends CaseInstanceRef with Cleanable {
}