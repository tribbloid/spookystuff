package com.tribbloids.spookystuff.uav.planning

import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.uav.UAVConf
import com.tribbloids.spookystuff.uav.actions.{UAVAction, UAVNavigation}
import com.tribbloids.spookystuff.uav.spatial.Location
import com.tribbloids.spookystuff.uav.telemetry.Link.Booking
import com.tribbloids.spookystuff.uav.telemetry.{Link, UAVStatus}
import com.tribbloids.spookystuff.utils.ShippingMarks

import scala.concurrent.duration.Duration

/**
  * useless in DSL, cannot be shipped, prepend by GenPartitioner only.
  * does NOT fail when the Link is unreachable (hence prefer), will try any available alternative instead.
  */
private[uav] case class PreferUAV(
                                   uavStatus: UAVStatus,
                                   bookedBy: Option[Booking] = None
                                 ) extends UAVAction with ShippingMarks {

  override def skeleton = None

  override def doExe(session: Session) = {
    assert(notShipped, "cannot execute after shipping")

    val fleet = session.spooky.getConf[UAVConf].uavsInFleetShuffled
    assert(fleet.contains(uavStatus.uav), "cannot prefer UAV not in the fleet")

    val link = uavStatus.uav.getLink(session.spooky)
    for (
      b1 <- link._bookedBy;
      b2 <- this.bookedBy
    ) {
      assert(b1 == b2, "cannot unbook due to ID mismatch")
      link._bookedBy = None
    }

    Link.select(
      fleet,
      session,
      {
        seq =>
          val exact = seq.find(_.uav == uavStatus.uav)
          val result = exact.orElse(seq.headOption)
          result
      }
    )

    Nil
  }
}

private[uav] case class WaypointPlaceholder(
                                             _to: Location
                                           ) extends UAVNavigation {

  override def delay: Duration = Duration.Zero

  override def exeNoOutput(session: Session): Unit = {
    Nil
  }
}
