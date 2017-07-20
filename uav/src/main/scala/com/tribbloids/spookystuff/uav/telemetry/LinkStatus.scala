package com.tribbloids.spookystuff.uav.telemetry

import com.tribbloids.spookystuff.session.LifespanContext
import com.tribbloids.spookystuff.uav.UAVConf
import com.tribbloids.spookystuff.uav.spatial.Location
import com.tribbloids.spookystuff.uav.system.UAV
import com.tribbloids.spookystuff.utils.IDMixin

/**
  * Created by peng on 24/02/17.
  * Only used in tests
  */
case class LinkStatus(
                       uav: UAV,
                       ownerOpt: Option[LifespanContext],
                       home: Location = UAVConf.DEFAULT_HOME_LOCATION,
                       currentLocation: Location = UAVConf.DEFAULT_HOME_LOCATION
                     ) extends IDMixin {

  override def _id: Any = uav
}
