package com.tribbloids.spookystuff.uav.planning

import java.util.UUID

import com.tribbloids.spookystuff.row.SpookySchema
import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.uav.UAVConf
import com.tribbloids.spookystuff.uav.actions.UAVNavigation
import com.tribbloids.spookystuff.uav.spatial.point.Location
import com.tribbloids.spookystuff.uav.telemetry.{Dispatcher, LinkStatus}
import com.tribbloids.spookystuff.uav.utils.Lock
import com.tribbloids.spookystuff.utils.ShippingMarks

import scala.concurrent.duration.Duration

/**
  * useless in DSL, cannot be shipped, prepend by GenPartitioner only.
  * does NOT fail when the Link is unreachable (hence prefer), will try any available alternative instead.
  */
private[uav] case class PreferUAV(
    uavStatus: LinkStatus,
    lockIDOpt: Option[UUID] = None
) extends UAVNavigation
    with ShippingMarks {

  override val cooldown: Duration = Duration.Zero

  override def skeleton = None

  override def doExe(session: Session) = {
    assert(notShipped, "cannot execute after shipping")

    val fleet = session.spooky.getConf[UAVConf].uavsInFleetShuffled
    assert(fleet.contains(uavStatus.uav), "cannot prefer UAV not in the fleet")

    Dispatcher(
      List(uavStatus.uav),
      session,
      Lock.Transient(lockIDOpt)
    )

    Nil
  }

  override def getLocation(schema: SpookySchema): Location = uavStatus.currentLocation
}
