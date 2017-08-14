package com.tribbloids.spookystuff.uav.actions

import com.tribbloids.spookystuff.extractors.impl.Lit
import com.tribbloids.spookystuff.extractors.{Col, Extractor, FR}
import com.tribbloids.spookystuff.row.{DataRowSchema, FetchedRow}
import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.uav.{UAVConf, UAVConst}
import com.tribbloids.spookystuff.uav.spatial.Location
import org.slf4j.LoggerFactory

import scala.concurrent.duration.Duration

/**
  * Created by peng on 26/08/16.
  */
@Deprecated // use waypoint
case class Move(
                 from: Col[Location],
                 to: Col[Location],
                 override val delay: Duration = UAVConst.UAVNavigation.delayMin
               ) extends WaypointLike {

  override def _from: Location = from.value

  override def doInterpolate(pageRow: FetchedRow, schema: DataRowSchema): Option[this.type] = {
    val fromVOpt = from.resolve(schema).lift.apply(pageRow)
    val toOpt = to.resolve(schema).lift.apply(pageRow)
    val result = for(
      fromV <- fromVOpt;
      toV <- toOpt
    ) yield {
      this.copy(
        from = Lit(Location.parse(fromV, schema.spooky.getConf[UAVConf])),
        to = Lit(Location.parse(toV, schema.spooky.getConf[UAVConf]))
      )
    }
    result.map(_.asInstanceOf[this.type])
  }

  override def getSessionView(session: Session) = new this.SessionView(session)

  class SessionView(session: Session) extends super.SessionView(session) {

    override def inbound(): Unit = {
      super.inbound()
      LoggerFactory.getLogger(this.getClass).debug(s"inbound .. ${_from}")
      link.synch.goto(_from)
    }
  }

  override def encode = ???
}