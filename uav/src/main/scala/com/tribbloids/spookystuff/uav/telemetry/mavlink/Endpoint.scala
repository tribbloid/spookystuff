package com.tribbloids.spookystuff.uav.telemetry.mavlink

import com.tribbloids.spookystuff.session.python._
import com.tribbloids.spookystuff.uav.UAVConf

/**
  * Created by peng on 16/01/17.
  */
case class Endpoint(
                     uri: String,
                     frame: Option[String] = None,
                     baudRate: Int = UAVConf.DEFAULT_BAUDRATE,
                     groundSSID: Int = UAVConf.EXECUTOR_SSID,
                     name: String = "DRONE"
                   )(
                     override val driverTemplate: PythonDriver
                   ) extends CaseInstanceRef
  //  with ConflictDetection
  with BindedRef {

  //  override lazy val _resourceIDs = Map("" -> Set(uri))
}
