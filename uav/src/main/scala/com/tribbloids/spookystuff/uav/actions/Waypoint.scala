package com.tribbloids.spookystuff.uav.actions

import com.tribbloids.spookystuff.extractors.Col
import com.tribbloids.spookystuff.extractors.impl.Lit
import com.tribbloids.spookystuff.row.{SpookySchema, FetchedRow}
import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.uav.spatial.Anchors
import com.tribbloids.spookystuff.uav.spatial.point.{Location, NED}
import com.tribbloids.spookystuff.uav.{UAVConf, UAVConst}
import org.apache.spark.mllib.uav.Vec
import org.slf4j.LoggerFactory

import scala.concurrent.duration.Duration

/**
  * Created by peng on 18/12/16.
  */
trait WaypointLike extends UAVNavigation {

  val to: Col[Location]
  lazy val _to = to.value

  override def getLocation(schema: SpookySchema): Location = _to

  override def getSessionView(session: Session) = new this.SessionView(session)

  class SessionView(session: Session) extends NavSessionView(session) {

    override def engage(): Unit = {
      LoggerFactory.getLogger(this.getClass).info(s"moving to $to")
      link.synch.goto(_to)
    }
  }
}

// How to accommodate camera & gimbal control? Right now do not refactor! Simplicity first.
case class Waypoint(
                     override val to: Col[Location],
                     override val delay: Duration = UAVConst.UAVNavigation.delayMin
                   ) extends WaypointLike {

  override def doInterpolate(
                              pageRow: FetchedRow,
                              schema: SpookySchema
                            ): Option[this.type] = {

    val vOpt: Option[Any] = to.resolve(schema).lift(pageRow)

    val uavConf = schema.spooky.getConf[UAVConf]

    vOpt.map {
      v =>
        val p = Location.parse(v)
          .replaceAnchors {
            case Anchors.Home =>
              uavConf._home
          }
        this.copy(
          to = Lit(p)
        )
          .asInstanceOf[this.type]
    }
  }

  override def shift(vector: Vec): this.type = {
    val shifted: Location = NED.fromVec(vector) -> _to
    this.copy(to = shifted).asInstanceOf[this.type]
  }
}

object WP {
  def apply = Waypoint.apply _
}