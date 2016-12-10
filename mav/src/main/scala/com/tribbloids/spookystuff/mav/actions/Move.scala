package com.tribbloids.spookystuff.mav.actions

import com.tribbloids.spookystuff.extractors.{Extractor, FR, Literal}
import com.tribbloids.spookystuff.row.{DataRowSchema, FetchedRow}
import com.tribbloids.spookystuff.session.Session

import scala.concurrent.duration.Duration

/**
  * Created by peng on 26/08/16.
  * Go to point1 then go to point2, end of.
  */
// How to accommodate camera & gimbal control? Right now do not refactor! Simplicity first.
case class Move(
                 from: Extractor[WayPoint],
                 to: Extractor[WayPoint],
                 override val delay: Duration = null
               ) extends MAVInteraction {

  lazy val fromV = from.asInstanceOf[Literal[FR, WayPoint]].value
  lazy val toV = to.asInstanceOf[Literal[FR, WayPoint]].value

  override def doInterpolate(pageRow: FetchedRow, schema: DataRowSchema): Option[this.type] = {
    val fromOpt = from.resolve(schema).lift.apply(pageRow)
    val toOpt = to.resolve(schema).lift.apply(pageRow)
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
    pyLink.assureClearanceAltitude(mavConf.takeOffAltitude)
    pyLink.move(self.fromV)
  }

  override def conduct(): Unit = {
    pyLink.move(self.toV)
  }
}