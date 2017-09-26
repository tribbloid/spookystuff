package com.tribbloids.spookystuff.uav.planning

import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.uav.UAVConf
import com.tribbloids.spookystuff.uav.actions.UAVNavigation
import com.tribbloids.spookystuff.uav.actions.mixin.HasExactLocation
import com.tribbloids.spookystuff.uav.telemetry.{Link, UAVStatus}
import com.tribbloids.spookystuff.utils.ShippingMarks

/**
  * useless in DSL, cannot be shipped, prepend by GenPartitioner only.
  * does NOT fail when the Link is unreachable (hence prefer), will try any available alternative instead.
  */
private[uav] case class PreferUAV(
                                   uavStatus: UAVStatus,
                                   mutexIDOpt: Option[Long] = None
                                 ) extends UAVNavigation
  with HasExactLocation
  with ShippingMarks {

  override def skeleton = None

  override def doExe(session: Session) = {
    assert(notShipped, "cannot execute after shipping")

    val fleet = session.spooky.getConf[UAVConf].uavsInFleetShuffled
    assert(fleet.contains(uavStatus.uav), "cannot prefer UAV not in the fleet")

    Link.Selector.withMutex(
      fleet,
      session,
      mutexIDOpt
    )

    Nil
  }

  override def _start = uavStatus.currentLocation
}
