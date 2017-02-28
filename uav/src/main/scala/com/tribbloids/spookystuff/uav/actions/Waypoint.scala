package com.tribbloids.spookystuff.uav.actions

import com.tribbloids.spookystuff.extractors.{Extractor, FR, Literal}
import com.tribbloids.spookystuff.uav.UAVConf
import com.tribbloids.spookystuff.row.{DataRowSchema, FetchedRow}
import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.uav.spatial.Location
import org.slf4j.LoggerFactory

import scala.concurrent.duration.Duration

/**
  * Created by peng on 18/12/16.
  */
trait WaypointLike extends UAVNavigation {

  val to: Extractor[Any]
  lazy val _to = to.asInstanceOf[Literal[FR, Location]].value

  override def getSessionView(session: Session) = new this.SessionView(session)

  class SessionView(session: Session) extends super.SessionView(session) {

    override def inbound(): Unit = {
      LoggerFactory.getLogger(this.getClass).debug(s"assureClearanceAltitude ${mavConf.clearanceAltitude}")
      link.synch.clearanceAlt(mavConf.clearanceAltitude)
    }

    override def engage(): Unit = {
      LoggerFactory.getLogger(this.getClass).info(s"scanning .. ${_to}")
      link.synch.move(_to)
    }
  }
}

case class Waypoint(
                     to: Extractor[Any],
                     override val delay: Duration = null
                   ) extends WaypointLike {

  override def doInterpolate(pageRow: FetchedRow, schema: DataRowSchema): Option[this.type] = {
    val vOpt: Option[Any] = to.resolve(schema).lift
      .apply(pageRow)

    vOpt.map {
      v =>
        val p = Location.parse(v, schema.spooky.submodule[UAVConf])
        this.copy(
          to = Literal(p)
        )
          .asInstanceOf[this.type ]
    }
  }

  override def start: Location = _to
  override def end: Location = _to
}
