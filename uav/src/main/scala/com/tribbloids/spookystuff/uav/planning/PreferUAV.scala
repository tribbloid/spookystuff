package com.tribbloids.spookystuff.uav.planning

import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.uav.actions.{UAVAction, UAVNavigation}
import com.tribbloids.spookystuff.uav.spatial.Location
import com.tribbloids.spookystuff.uav.telemetry.UAVStatus
import com.tribbloids.spookystuff.utils.ShippingMarks

import scala.concurrent.duration.Duration

/**
  * useless in DSL, cannot be shipped, prepend by GenPartitioner only.
  * does NOT fail when the Link is unreachable (hence prefer), will try any available alternative instead.
  */
private[uav] case class PreferUAV(
                                   uavs: UAVStatus*
                                 ) extends UAVAction with ShippingMarks {

  override def skeleton = None

  override def doExe(session: Session) = {
    assert(notShipped, "cannot execute after shipping")
    //TODO: assert the same UAV is acquired by current task
    Nil
  }
}

private[uav] case class WrapLocation(
                                      _to: Location
                                    ) extends UAVNavigation {

  override def delay: Duration = Duration.Zero

  override def exeNoOutput(session: Session): Unit = {
    Nil
  }
}