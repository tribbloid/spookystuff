package com.tribbloids.spookystuff.mav.actions

import com.tribbloids.spookystuff.extractors.{Extractor, FR, Literal}
import com.tribbloids.spookystuff.row.{DataRowSchema, FetchedRow}
import com.tribbloids.spookystuff.session.Session
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
               ) extends MAVInteraction {

  lazy val fromV = from.asInstanceOf[Literal[FR, Location]].value
  lazy val toV = to.asInstanceOf[Literal[FR, Location]].value

  override def doInterpolate(pageRow: FetchedRow, schema: DataRowSchema): Option[this.type] = {
    val fromOpt = from.asInstanceOf[Extractor[Location]].resolve(schema).lift.apply(pageRow)
    val toOpt = to.asInstanceOf[Extractor[Location]].resolve(schema).lift.apply(pageRow)
    val result = for(
      fromV <- fromOpt;
      toV <- toOpt
    ) yield {
      this.copy(
        from = Literal(fromV),
        to = Literal(toV)
      )
    }
    result.map(_.asInstanceOf[this.type])
  }

  override def getImpl(session: Session): MAVImpl = MoveImpl(this, session)
}

case class MoveImpl(
                     self: Move,
                     session: Session
                   ) extends MAVImpl(session) {

  override def inbound(): Unit = {
    LoggerFactory.getLogger(this.getClass).info(s"assureClearanceAltitude ${mavConf.takeOffAltitude}")
    pyLink.assureClearanceAltitude(mavConf.takeOffAltitude)
    LoggerFactory.getLogger(this.getClass).info(s"inbound .. ${self.fromV}")
    pyLink.move(self.fromV)
  }

  override def conduct(): Unit = {
    LoggerFactory.getLogger(this.getClass).info(s"scanning .. ${self.toV}")
    pyLink.move(self.toV)
  }
}