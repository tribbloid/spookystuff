package com.tribbloids.spookystuff.uav.actions

import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.uav.spatial.{Anchor, Anchors, Location, NED}

/**
  * won't do a thing if already in the air.
  */
private[uav] case class Takeoff(
                                 altitude: Long,
                                 to: Location
                               ) extends UAVNavigation {

  override def _end = to

  override protected def exeNoOutput(session: Session) = {


  }

  override def replaceAnchor(fn: PartialFunction[Anchor, Anchor]) = this.copy(
    to = to.replaceAnchors(fn)
  ).asInstanceOf[this.type]
}

object Takeoff {

  def apply(altitude: Long = 0): Takeoff = Takeoff(
    altitude,
    Location.fromTuple(NED.V(0, 0, - altitude) -> Anchors.HomeLevelProjection)
  )
}
