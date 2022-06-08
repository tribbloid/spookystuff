package com.tribbloids.spookystuff.uav.system

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.uav.UAVConf
import com.tribbloids.spookystuff.uav.telemetry.Link

/**
  * Created by peng on 15/01/17.
  */
case class UAV(
    // remember, one drone can have several telemetry
    // endpoints: 1 primary and several backups (e.g. text message-based)
    // TODO: implement telemetry backup mechanism, can use MAVproxy's multiplexing feature
    uris: Seq[String], // [protocol]:ip:port;[baudRate]
    frame: Option[String] = None,
    baudRate: Int = UAVConf.DEFAULT_BAUDRATE,
    groundSSID: Int = UAVConf.EXECUTOR_SSID,
    protocol: Protocol = Protocol.MAVLink, //currently useless.
    name: String = "DRONE"
) {

  val primaryURI = uris.head
  val fullName = s"${(Seq(name) ++ frame.toSeq).mkString(":")}@${uris.head}"

  def getLink(
      spooky: SpookyContext
  ): Link = {

    var newLink: Boolean = false
    val link = Link.synchronized {
      Link.registered.getOrElse(
        this, {
          val factory = spooky.getConf[UAVConf].routing
          val link = factory.apply(this)
          link.register(
            spooky,
            factory
          )
          newLink = true
          link
        }
      )
    }

    link
  }

  override def toString = fullName
}
