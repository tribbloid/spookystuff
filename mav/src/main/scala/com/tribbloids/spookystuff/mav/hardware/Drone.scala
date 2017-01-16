package com.tribbloids.spookystuff.mav.hardware

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.mav.MAVConf
import com.tribbloids.spookystuff.mav.actions._
import com.tribbloids.spookystuff.mav.telemetry.{Endpoint, Link}

/**
  * Created by peng on 15/01/17.
  */
case class Drone(
                  // remember, one drone can have several telemetry
                  // endpoints: 1 primary and several backups (e.g. text message-based)
                  // TODO: implement telemetry backup mechanism, can use MAVproxy's multiple master feature
                  uris: Seq[String], // [protocol]:ip:port;[baudRate]
                  frame: Option[String] = None,
                  baudRate: Int = MAVConf.DEFAULT_BAUDRATE,
                  endpointSSID: Int = MAVConf.EXECUTOR_SSID,
                  name: String = "DRONE"
                ) {

  def getDirectEndpoint = Endpoint(
    uris.head,
    baudRate,
    endpointSSID,
    frame
  )

  override def toString = s"${(Seq(name) ++ frame.toSeq).mkString(":")}@${uris.head}"

  var home: Option[LocationGlobal] = None
  var location: Option[LocationGlobal] = None

  def updateStatus(spooky: SpookyContext): LocationGlobal = {
    spooky.withSession {
      session =>
        val link = Link.getOrInitialize(Seq(this), spooky.conf.submodule[MAVConf].linkFactory, session)
        link.getHome
        link.getLocation
    }
  }
}
