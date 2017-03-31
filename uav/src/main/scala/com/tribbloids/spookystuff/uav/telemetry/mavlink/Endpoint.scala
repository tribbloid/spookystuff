package com.tribbloids.spookystuff.uav.telemetry.mavlink

import com.tribbloids.spookystuff.uav.UAVConf
import com.tribbloids.spookystuff.session.DetectResourceConflict
import com.tribbloids.spookystuff.session.python._

/**
  * Created by peng on 16/01/17.
  */
case class Endpoint(
                     uri: String, // [protocol]:ip:port;[baudRate]
                     baudRate: Int = UAVConf.DEFAULT_BAUDRATE,
                     ssid: Int = UAVConf.EXECUTOR_SSID,
                     frame: Option[String] = None
                   ) extends CaseInstanceRef with BindedRef with DetectResourceConflict {

  override lazy val resourceIDs = Map("" -> Set(uri))
}
