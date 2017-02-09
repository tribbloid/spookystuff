package com.tribbloids.spookystuff.uav.actions

import com.tribbloids.spookystuff.extractors.{Extractor, FR, Literal}
import com.tribbloids.spookystuff.row.{DataRowSchema, FetchedRow}
import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.uav.UAVConf
import com.tribbloids.spookystuff.uav.spatial.LocationGlobal
import org.slf4j.LoggerFactory

import scala.concurrent.duration.Duration

/**
  * Created by peng on 26/08/16.
  * Go to point1 then go to point2, end of.
  */
// How to accommodate camera & gimbal control? Right now do not refactor! Simplicity first.
case class Move(
                 from: Extractor[Any],
                 to: Extractor[Any],
                 override val delay: Duration = null
               ) extends AbstractGoto {

  override def doInterpolate(pageRow: FetchedRow, schema: DataRowSchema): Option[this.type] = {
    val fromVOpt = from.asInstanceOf[Extractor[LocationGlobal]].resolve(schema).lift.apply(pageRow)
    val toOpt = to.asInstanceOf[Extractor[LocationGlobal]].resolve(schema).lift.apply(pageRow)
    val result = for(
      fromV <- fromVOpt;
      toV <- toOpt
    ) yield {
      this.copy(
        from = Literal(fromV),
        to = Literal(toV)
      )
    }
    result.map(_.asInstanceOf[this.type])
  }

  override def getSessionView(session: Session) = new this.SessionView(session)

  class SessionView(session: Session) extends super.SessionView(session) {

    lazy val fromLocation: LocationGlobal = from.asInstanceOf[Literal[FR, LocationGlobal]].value
      .toGlobal(Some(session.spooky.conf.submodule[UAVConf].locationReference))

    override def inbound(): Unit = {
      super.inbound()
      LoggerFactory.getLogger(this.getClass).debug(s"inbound .. $fromLocation")
      link.Synch.move(fromLocation)
    }
  }

  override def start_end: Seq[(LocationGlobal, LocationGlobal)] = Nil
}