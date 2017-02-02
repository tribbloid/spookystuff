package com.tribbloids.spookystuff.uav.telemetry.mavlink

import com.tribbloids.spookystuff.uav.MAVConf
import com.tribbloids.spookystuff.session.ResourceLedger
import com.tribbloids.spookystuff.session.python._

/**
  * Created by peng on 16/01/17.
  */
case class Endpoint(
                     uri: String, // [protocol]:ip:port;[baudRate]
                     baudRate: Int = MAVConf.DEFAULT_BAUDRATE,
                     ssid: Int = MAVConf.EXECUTOR_SSID,
                     frame: Option[String] = None
                   ) extends CaseInstanceRef with BijectoryRef with ResourceLedger {

  override lazy val resourceIDs = Map("" -> Set(uri))
}
