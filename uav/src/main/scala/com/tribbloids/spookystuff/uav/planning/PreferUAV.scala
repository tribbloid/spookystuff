package com.tribbloids.spookystuff.uav.planning

import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.uav.actions.UAVNavigation
import com.tribbloids.spookystuff.uav.spatial.Location
import com.tribbloids.spookystuff.uav.telemetry.{Link, UAVStatus}

import scala.concurrent.duration.Duration

/**
  * useless in DSL, cannot be shipped, prepend by GenPartitioner only.
  * does NOT fail when the Link is unreachable (hence prefer), will try any available alternative instead.
  */
private[uav] case class PreferUAV(
                                   @transient links: Link*
                                 ) extends UAVNavigation {

  val statuses: Seq[UAVStatus] = links.map(_.status())

  def bestLink = links.head

  override def skeleton = None

  override def exeNoOutput(session: Session): Unit = {
    assert(links != null, "cannot execute after shipping")
    Nil
  }

  override def _to: Location = {
    val firstLocation = bestLink.status().currentLocation
    firstLocation
  }

  override def delay: Duration = Duration.Zero
}

private[uav] case class WrapLocation(
                                      _to: Location
                                    ) extends UAVNavigation {

  override def delay: Duration = Duration.Zero

  override def exeNoOutput(session: Session): Unit = {
    Nil
  }
}