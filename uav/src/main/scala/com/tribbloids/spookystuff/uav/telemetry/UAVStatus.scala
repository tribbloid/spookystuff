package com.tribbloids.spookystuff.uav.telemetry
import com.tribbloids.spookystuff.uav.spatial.Location
import com.tribbloids.spookystuff.uav.system.UAV

/**
  * All should be non-blocking
  */
case class UAVStatus(
                       uav: UAV,
                       home: Location,
                       currentLocation: Location
                     ) {
}
