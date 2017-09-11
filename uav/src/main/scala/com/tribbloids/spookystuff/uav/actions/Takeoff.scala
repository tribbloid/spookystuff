package com.tribbloids.spookystuff.uav.actions

import com.tribbloids.spookystuff.extractors.Col
import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.uav.spatial.{Anchor, Anchors, Location, NED}
import org.slf4j.LoggerFactory

/**
  * won't do a thing if already in the air.
  */
case class Takeoff(
                    altitude: Col[Long],
                    private val to: Location
                  ) extends UAVNavigation {

  override def _end = to

  override def doReplaceAnchors(fn: PartialFunction[Anchor, Anchor]) = this.copy(
    to = to.replaceAnchors(fn)
  ).asInstanceOf[this.type]

  override def getSessionView(session: Session) = new this.SessionView(session)

  implicit class SessionView(session: Session) extends super.SessionView(session) {

    lazy val _alt = if (altitude.value > 0) altitude.value
    else uavConf.clearanceAltitudeMin

    override def engage(): Unit = {

      LoggerFactory.getLogger(this.getClass)
        .info(s"taking off and climbing to ${_alt}")

      link.synch.clearanceAlt(_alt)
    }
  }
}

object Takeoff {

  def apply(altitude: Long = 0): Takeoff = Takeoff(
    altitude,
    Location.fromTuple(NED.V(0, 0, - altitude) -> Anchors.HomeLevelProjection)
  )
}
