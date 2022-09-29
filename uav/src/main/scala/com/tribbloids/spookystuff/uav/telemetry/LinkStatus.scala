package com.tribbloids.spookystuff.uav.telemetry

import com.tribbloids.spookystuff.uav.UAVConf
import com.tribbloids.spookystuff.uav.spatial.point.Location
import com.tribbloids.spookystuff.uav.system.UAV
import com.tribbloids.spookystuff.uav.utils.Binding
import com.tribbloids.spookystuff.utils.IDMixin

/**
  * Created by peng on 24/02/17. Link is not shippable But LinkStatus is!
  */
case class LinkStatus(
    uav: UAV,
    lock: Binding,
    home: Location = UAVConf.DEFAULT_HOME_LOCATION,
    currentLocation: Location = UAVConf.DEFAULT_HOME_LOCATION
) extends IDMixin {

  override def _id: Any = uav

  val lockStr = lock.toString
}
