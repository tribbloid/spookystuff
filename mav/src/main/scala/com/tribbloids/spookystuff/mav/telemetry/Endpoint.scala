package com.tribbloids.spookystuff.mav.telemetry

import com.tribbloids.spookystuff.mav.MAVConf
import com.tribbloids.spookystuff.session.python._
import com.tribbloids.spookystuff.session.{LocalCleanable, ResourceLock}
/**
  * Created by peng on 16/01/17.
  */
case class Endpoint(
                     uri: String, // [protocol]:ip:port;[baudRate]
                     baudRate: Int = MAVConf.DEFAULT_BAUDRATE,
                     ssid: Int = MAVConf.EXECUTOR_SSID,
                     frame: Option[String] = None
                   ) extends CaseInstanceRef with SingletonRef with LocalCleanable with ResourceLock {

  override lazy val resourceIDs = Map("" -> Set(uri))
}
