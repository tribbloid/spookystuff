package com.tribbloids.spookystuff.uav.telemetry.mavlink

import com.tribbloids.spookystuff.uav.UAVConf
import com.tribbloids.spookystuff.session.DetectResourceConflict
import com.tribbloids.spookystuff.session.python._

/**
  * Created by peng on 16/01/17.
  */
case class Endpoint(
                     uri: String,
                     frame: Option[String] = None,
                     baudRate: Int = UAVConf.DEFAULT_BAUDRATE,
                     groundSSID: Int = UAVConf.EXECUTOR_SSID,
                     name: String = "DRONE"
                   ) extends CaseInstanceRef
  with BindedRef
  with DetectResourceConflict {

  override lazy val resourceIDs = Map("" -> Set(uri))
}
