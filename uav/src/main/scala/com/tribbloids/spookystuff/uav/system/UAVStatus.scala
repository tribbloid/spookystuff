package com.tribbloids.spookystuff.uav.system

import com.tribbloids.spookystuff.uav.UAVConf
import com.tribbloids.spookystuff.uav.spatial.point.Location
import com.tribbloids.spookystuff.utils.IDMixin
import com.tribbloids.spookystuff.utils.lifespan.LifespanContext

/**
  * Created by peng on 24/02/17.
  * Only used in tests
  */
case class UAVStatus(
                      uav: UAV,
                      ownerOpt: Option[LifespanContext],
                      home: Location = UAVConf.DEFAULT_HOME_LOCATION,
                      currentLocation: Location = UAVConf.DEFAULT_HOME_LOCATION
                    ) extends IDMixin {

  override def _id: Any = uav
}
