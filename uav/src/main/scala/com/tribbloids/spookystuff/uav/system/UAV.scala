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
                // TODO: implement telemetry backup mechanism, can use MAVproxy's multiple master feature
                uris: Seq[String], // [protocol]:ip:port;[baudRate]
                frame: Option[String] = None,
                baudRate: Int = UAVConf.DEFAULT_BAUDRATE,
                endpointSSID: Int = UAVConf.EXECUTOR_SSID,
                name: String = "DRONE"
              ) {

  def getLink(
               spooky: SpookyContext,
               tryConnect: Boolean = true
             ): Link = {

    var newLink: Boolean = false
    val link = Link.synchronized {
      Link.existing.getOrElse(
        this,
        {
          val factory = spooky.conf.submodule[UAVConf].linkFactory
          val link = factory.apply(this)
          link.setContext(
            spooky,
            factory
          )
          newLink = true
          link
        }
      )
    }
    if (newLink && tryConnect) try {
      link.connect()
    }
    catch {
      case e: Throwable =>
    }

    link
  }

  override def toString = s"${(Seq(name) ++ frame.toSeq).mkString(":")}@${uris.head}"
}
