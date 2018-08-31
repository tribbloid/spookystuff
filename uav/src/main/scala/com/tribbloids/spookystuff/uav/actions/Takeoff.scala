package com.tribbloids.spookystuff.uav.actions

import com.tribbloids.spookystuff.Const
import com.tribbloids.spookystuff.extractors.Col
import com.tribbloids.spookystuff.row.{FetchedRow, SpookySchema}
import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.uav.UAVConf
import com.tribbloids.spookystuff.uav.spatial.point.{Location, NED}
import com.tribbloids.spookystuff.utils.SpookyUtils
import org.apache.spark.ml.uav.Vec
import org.slf4j.LoggerFactory

import scala.concurrent.duration.Duration

/**
  * won't do a thing if already in the air.
  */
//old design is unable to evolve Takeoff of which exact location depends on previous nav
//in case of ...WP1)-(Takeoff-WP2..., both has violations, each update then becomes:
//  WP1-Takeoff: No violation
//  Takeoff-WP2: Has violation but Takeoff cannot be lowered.

//TODO: change to be like this: wrap a Nav, if on the ground, arm and takeoff, if in the air, serve as an altitude lower bound
case class Takeoff(
    minAlt: Col[Double] = -1,
    maxAlt: Col[Double] = -1,
    override val cooldown: Duration = Const.Interaction.delayMin,
    prevNavOpt: Option[UAVNavigation] = None
) extends UAVNavigation {

  override def doInterpolate(row: FetchedRow, schema: SpookySchema): Option[this.type] = {
    val uav = schema.spooky.getConf[UAVConf]
    val minAltOpt = this.minAlt.resolve(schema).lift(row).flatMap(SpookyUtils.asOption[Double])
    val maxAltOpt = this.minAlt.resolve(schema).lift(row).flatMap(SpookyUtils.asOption[Double])

    minAltOpt -> maxAltOpt match {
      case (Some(min), Some(max)) =>
        val _min =
          if (min > 0) min
          else uav.takeoffMinAltitude

        val _max =
          if (max > 0) max
          else uav.takeoffMaxAltitude

        assert(min <= max, s"minAlt $min should < maxAlt $max")
        Some(this.copy(minAlt = _min, maxAlt = _max).asInstanceOf[this.type])
      case _ =>
        None
    }
  }

  /**
    * inserted by GenPartitioner for path calculation
    */
  def prevNav = prevNavOpt.getOrElse {
    throw new UnsupportedOperationException("prevNavOpt is unset")
  }

  override def getSessionView(session: Session) = new this.SessionView(session)

  class SessionView(session: Session) extends NavSessionView(session) {

    validateValues()

    override def engage(): Unit = {

      LoggerFactory
        .getLogger(this.getClass)
        .info(s"taking off and climbing to ${minAlt.value} ~ ${maxAlt.value}")

      link.synchAPI.clearanceAlt(minAlt.value)
    }
  }

  private def validateValues() = {

    val min = minAlt.value
    val max = maxAlt.value

    assert(min > 0, s"minAlt $min should > 0")
    assert(max > 0, s"maxAlt $max should > 0")
    assert(min <= max, s"minAlt $min should < maxAlt $max")

  }

  override def getLocation(schema: SpookySchema) = {
    validateValues()

    val spooky = schema.ec.spooky
    val uavConf = spooky.getConf[UAVConf]
    //    def fallbackLocation = Location.fromTuple(NED.C(0,0,-minAlt) -> Anchors.HomeLevelProjection)

    val prevLocation = prevNav.getEnd(schema)
    val coord = prevLocation.coordinate(NED, uavConf._home)
    val alt = -coord.down
    val objAlt = Math.min(Math.max(alt, minAlt.value), maxAlt.value)
    Location.fromTuple(coord.copy(down = -objAlt) -> uavConf._home)
  }

  /**
    * can only increase minAlt or decrease MaxAlt
    * @param vector
    * @return
    */
  override def shift(vector: Vec): this.type = {
    val dAlt = vector(2)
    val result = if (dAlt > 0) {
      val newMinAlt = minAlt.value + dAlt
      val newMaxAlt = Math.max(maxAlt.value, newMinAlt)
      this.copy(
        minAlt = newMinAlt,
        maxAlt = newMaxAlt
      )
    } else if (dAlt < 0) {
      val newMaxAlt = maxAlt.value + dAlt
      val newMinAlt = Math.min(minAlt.value, newMaxAlt)
      this.copy(
        minAlt = newMinAlt,
        maxAlt = newMaxAlt
      )
    } else {
      this
    }
    result.asInstanceOf[this.type]
  }
}
