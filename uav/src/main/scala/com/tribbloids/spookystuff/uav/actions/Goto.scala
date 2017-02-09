package com.tribbloids.spookystuff.uav.actions

import com.tribbloids.spookystuff.extractors.{Extractor, FR, Literal}
import com.tribbloids.spookystuff.uav.UAVConf
import com.tribbloids.spookystuff.row.{DataRowSchema, FetchedRow}
import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.uav.spatial.LocationGlobal
import org.slf4j.LoggerFactory

import scala.concurrent.duration.Duration

/**
  * Created by peng on 18/12/16.
  */
trait AbstractGoto extends UAVPositioning {

  val to: Extractor[Any]
  lazy val _to = to.asInstanceOf[Literal[FR, LocationGlobal]].value

  override def getSessionView(session: Session) = new this.SessionView(session)

  class SessionView(session: Session) extends super.SessionView(session) {

    override def inbound(): Unit = {
      LoggerFactory.getLogger(this.getClass).debug(s"assureClearanceAltitude ${mavConf.clearanceAltitude}")
      link.Synch.clearanceAlt(mavConf.clearanceAltitude)
    }

    override def engage(): Unit = {
      LoggerFactory.getLogger(this.getClass).info(s"scanning .. ${_to}")
      link.Synch.move(_to)
    }
  }
}

case class Goto(
                 to: Extractor[Any],
                 override val delay: Duration = null
               ) extends AbstractGoto {

  override def doInterpolate(pageRow: FetchedRow, schema: DataRowSchema): Option[this.type] = {
    val toOpt: Option[LocationGlobal] = to.asInstanceOf[Extractor[LocationGlobal]].resolve(schema).lift
      .apply(pageRow)
      .map {
        _.toGlobal(Some(schema.spooky.conf.submodule[UAVConf].locationReference))
      }
    val result = for(
      toV <- toOpt
    ) yield {
      this.copy(
        to = Literal(toV)
      )
    }
    result.map(_.asInstanceOf[this.type])
  }

  override def start_end: Seq[(LocationGlobal, LocationGlobal)] = {
    Nil
  }
}
