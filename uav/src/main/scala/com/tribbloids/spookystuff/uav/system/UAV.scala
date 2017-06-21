package com.tribbloids.spookystuff.uav.system

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.uav.UAVConf
import com.tribbloids.spookystuff.uav.telemetry.Link

import scala.util.Try

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
                name: String = "DRONE"
              ) {

  val primaryURI = uris.head

  def getLink(
               spooky: SpookyContext,
               tryConnect: Boolean = true
             ): Link = {

    var newLink: Boolean = false
    val link = Link.synchronized {
      Link.existing.getOrElse(
        this,
        {
          val factory = spooky.getConf[UAVConf].linkFactory
          val link = factory.apply(this)
          link.setFactory(
            spooky,
            factory
          )
          newLink = true
          link
        }
      )
    }
    if (newLink && tryConnect) Try {
      link.connect()
    }

    link
  }

  def fullID = {
    s"${(Seq(name) ++ frame.toSeq).mkString(":")}@${uris.head}"
  }

  override def toString = fullID
}
