package com.tribbloids.spookystuff.mav.system

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.mav.MAVConf
import com.tribbloids.spookystuff.mav.telemetry.Link

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

  def toLink(
              spooky: SpookyContext,
              tryConnect: Boolean = true
            ): Link = {

    var newLink: Boolean = false
    val link = Link.synchronized {
      Link.existing.getOrElse(
        this,
        {
          val factory = spooky.conf.submodule[MAVConf].linkFactory
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
