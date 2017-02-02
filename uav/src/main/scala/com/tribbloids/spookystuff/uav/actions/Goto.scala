package com.tribbloids.spookystuff.uav.actions

import com.tribbloids.spookystuff.extractors.{Extractor, FR, Literal}
import com.tribbloids.spookystuff.uav.MAVConf
import com.tribbloids.spookystuff.row.{DataRowSchema, FetchedRow}
import com.tribbloids.spookystuff.session.Session
import org.slf4j.LoggerFactory

import scala.concurrent.duration.Duration

/**
  * Created by peng on 18/12/16.
  */
trait AbstractGoto extends MAVInteraction {

  val to: Extractor[Any]

  override def getSessionView(session: Session) = new this.SessionView(session)

  class SessionView(session: Session) extends super.SessionView(session) {
    lazy val toLocation = to.asInstanceOf[Literal[FR, Location]].value
      .toGlobal(Some(session.spooky.conf.submodule[MAVConf].locationReference))

    override def inbound(): Unit = {
      LoggerFactory.getLogger(this.getClass).debug(s"assureClearanceAltitude ${mavConf.clearanceAltitude}")
      link.Synch.clearanceAlt(mavConf.clearanceAltitude)
    }

    override def conduct(): Unit = {
      LoggerFactory.getLogger(this.getClass).info(s"scanning .. $toLocation")
      link.Synch.move(toLocation)
    }
  }
}

case class Goto(
                 to: Extractor[Any],
                 override val delay: Duration = null
               ) extends AbstractGoto {

  override def doInterpolate(pageRow: FetchedRow, schema: DataRowSchema): Option[this.type] = {
    val toOpt = to.asInstanceOf[Extractor[Location]].resolve(schema).lift.apply(pageRow)
    val result = for(
      toV <- toOpt
    ) yield {
      this.copy(
        to = Literal(toV)
      )
    }
    result.map(_.asInstanceOf[this.type])
  }
}
