package com.tribbloids.spookystuff.mav.telemetry


import com.tribbloids.spookystuff.caching
import com.tribbloids.spookystuff.session.Cleanable
import com.tribbloids.spookystuff.session.python.CaseInstanceRef

object Proxy {

  val existing: caching.ConcurrentSet[Proxy] = caching.ConcurrentSet()

  def apply(
             master: String,
             outs: Seq[String]
           ): Proxy = {

    val result = Proxy(
      master,
      outs,
      "DRONE@" + master
    )
    result
  }

//  private def getPrimaryOut(port: Int) = {
//    s"udp:localhost:$port"
//  }
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

  Proxy.existing += this

  /**
    * no duplication due to port conflicts!
    */
  def primaryOut: String = outs.head

  override def cleanImpl(): Unit = {
    super.cleanImpl()
    Proxy.existing -= this
  }
}