package com.tribbloids.spookystuff.uav.actions

import com.tribbloids.spookystuff.actions.Trace
import com.tribbloids.spookystuff.extractors.Col
import com.tribbloids.spookystuff.row.DataRowSchema
import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.uav.UAVConf
import com.tribbloids.spookystuff.uav.spatial.{Anchors, Location, NED}
import org.slf4j.LoggerFactory

/**
  * won't do a thing if already in the air.
  */
//old design is unable to evolve Takeoff of which exact location depends on previous nav
//in case of ...WP1)-(Takeoff-WP2..., both has violations, each update then becomes:
//  WP1-Takeoff: No violation
//  Takeoff-WP2: Has violation but Takeoff cannot be lowered.

//TODO: this should become a wrapper, current design is very inefficient!
//TODO: change to be like this: wrap a Nav, if on the ground, arm and takeoff, if in the air, serve as an altitude lower bound
case class Takeoff(
                    altitude: Col[Double] = 1.0
                  ) extends UAVNavigation {

  override def getSessionView(session: Session) = new this.SessionView(session)

  implicit class SessionView(session: Session) extends NavSessionView(session) {

    val minAlt = getMinAlt(uavConf)

    override def engage(): Unit = {

      LoggerFactory.getLogger(this.getClass)
        .info(s"taking off and climbing to $minAlt")

      link.synch.clearanceAlt(minAlt)
    }
  }

  def getMinAlt(uavConf: UAVConf) = {
    if (altitude.value > 0) altitude.value
    else uavConf.clearanceAltitudeMin
  }
}

/**
  * inserted by GenPartitioner to bind with another Nav
  * Binding is the only option to ensure that both nav are treated as a rigid body, no deformity is allowed
  */
case class AndTakeoff(
                       prevNav: UAVNavigation,
                       takeoff: Takeoff
                     ) extends UAVNavigation {

  override def getSessionView(session: Session) = takeoff.getSessionView(session)

  override def getLocation(trace: Trace, schema: DataRowSchema) = {
    val spooky = schema.ec.spooky
    val uavConf = spooky.getConf[UAVConf]
    val minAlt = takeoff.getMinAlt(uavConf)

    def fallbackLocation = Location.fromTuple(NED.C(0,0,-minAlt) -> Anchors.HomeLevelProjection)

    val previousLocation = prevNav.getEnd(trace, schema)
    val previousCoordOpt = previousLocation.getCoordinate(NED, uavConf.home)
    val result = previousCoordOpt match {
      case Some(coord) =>
        val alt = -coord.down
        val objAlt = Math.max(alt, minAlt)
        Location.fromTuple(coord.copy(down = -objAlt) -> uavConf.home)
      case None =>
        fallbackLocation
    }
    result
  }
}