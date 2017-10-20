package com.tribbloids.spookystuff.uav.actions

import com.tribbloids.spookystuff.extractors.Col
import com.tribbloids.spookystuff.row.DataRowSchema
import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.uav.UAVConf
import com.tribbloids.spookystuff.uav.spatial.Anchors
import com.tribbloids.spookystuff.uav.spatial.point.{Location, NED}
import org.apache.spark.mllib.uav.Vec
import org.slf4j.LoggerFactory

/**
  * won't do a thing if already in the air.
  */
//old design is unable to evolve Takeoff of which exact location depends on previous nav
//in case of ...WP1)-(Takeoff-WP2..., both has violations, each update then becomes:
//  WP1-Takeoff: No violation
//  Takeoff-WP2: Has violation but Takeoff cannot be lowered.

//TODO: change to be like this: wrap a Nav, if on the ground, arm and takeoff, if in the air, serve as an altitude lower bound
case class Takeoff(
                    minAlt: Col[Double] = 1.0,
                    maxAlt: Col[Double] = 1.0,
                    prevNavOpt: Option[UAVNavigation] = None
                  ) extends UAVNavigation {

  /**
    * inserted by GenPartitioner for path calculation
    */
  def prevNav = prevNavOpt.getOrElse {
    throw new UnsupportedOperationException("prevNavOpt is unset")
  }

  override def getSessionView(session: Session) = new this.SessionView(session)

  implicit class SessionView(session: Session) extends NavSessionView(session) {

    val _minAlt = getMinAlt(uavConf)
    val _maxAlt = getMaxAlt(uavConf)

    override def engage(): Unit = {

      LoggerFactory.getLogger(this.getClass)
        .info(s"taking off and climbing to $_minAlt ~ $_maxAlt")

      link.synch.clearanceAlt(_minAlt)
    }
  }

  def getMinAlt(uavConf: UAVConf) = {
    if (minAlt.value > 0) minAlt.value
    else uavConf.takeoffMinAltitude
  }
  def getMaxAlt(uavConf: UAVConf) = {
    if (maxAlt.value > 0) maxAlt.value
    else uavConf.takeoffMaxAltitude
  }

  override def getLocation(schema: DataRowSchema) = {
    val spooky = schema.ec.spooky
    val uavConf = spooky.getConf[UAVConf]
    val minAlt = getMinAlt(uavConf)

    def fallbackLocation = Location.fromTuple(NED.C(0,0,-minAlt) -> Anchors.HomeLevelProjection)

    val previousLocation = prevNav.getEnd(schema)
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

  /**
    * can only increase altitude
    * @param vector
    * @return
    */
  override def shift(vector: Vec): this.type = {
    ???
  }
}