package com.tribbloids.spookystuff.uav.planning

import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.uav.UAVConf
import com.tribbloids.spookystuff.uav.actions.UAVNavigation
import com.tribbloids.spookystuff.uav.spatial.{Location, NED}
import com.tribbloids.spookystuff.uav.telemetry.{Link, UAVStatus}
import com.tribbloids.spookystuff.utils.ShippingMarks

/**
  * won't do a thing if already took off.
  */
private[uav] case class Takeoff(
                                 altitude: Long = 0
                               ) extends UAVNavigation {

  override def _to = throw new UnsupportedOperationException("cannot estimate location before casting to TakeoffWithUAV")

  override protected def exeNoOutput(session: Session) = {


  }
}

/**
  * useless in DSL, cannot be shipped, prepend by GenPartitioner only.
  * does NOT fail when the Link is unreachable (hence prefer), will try any available alternative instead.
  */
private[uav] case class TakeoffWithUAV(
                                        altitude: Long,
                                        home: Location,
                                        uavStatus: UAVStatus,
                                        mutexIDOpt: Option[Long] = None
                                      ) extends UAVNavigation with ShippingMarks {

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

  override def _from = uavStatus.currentLocation
  override def _to = {
    val currentAlt = _from.getCoordinate(from = home).get.alt
    val altDelta = altitude - currentAlt
    if (altDelta > 0 ) {
      Location.fromTuple(NED.V(0,0, -altDelta) -> _from)
    }
    else {
      _from
    }
  }
}