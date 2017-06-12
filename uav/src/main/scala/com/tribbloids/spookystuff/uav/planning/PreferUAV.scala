package com.tribbloids.spookystuff.uav.planning

import com.tribbloids.spookystuff.doc.Fetched
import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.uav.actions.UAVNavigation
import com.tribbloids.spookystuff.uav.spatial.Location
import com.tribbloids.spookystuff.uav.telemetry.Link
import com.tribbloids.spookystuff.utils.NOTSerializable

import scala.concurrent.duration.Duration

/**
  * useless in DSL, cannot be shipped, prepend by GenPartitioner only.
  * does NOT fail when the Link is unreachable (hence prefer), will trySelect another.
  */
private[uav] case class PreferUAV(
                                   links: Link*
                                 ) extends UAVNavigation with NOTSerializable {

  val firstLink = links.head

  override def skeleton = None

  override def exeNoOutput(session: Session): Unit = {
    Nil
  }

  override def _to: Location = {
    val firstLocation = firstLink.status().currentLocation
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