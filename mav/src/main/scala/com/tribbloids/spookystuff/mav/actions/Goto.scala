package com.tribbloids.spookystuff.mav.actions

import com.tribbloids.spookystuff.extractors.{Extractor, FR, Literal}
import com.tribbloids.spookystuff.mav.MAVConf
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
    lazy val toV = to.asInstanceOf[Literal[FR, Location]].value
      .toGlobal(session.spooky.conf.submodule[MAVConf].globalReference)

    override def conduct(): Unit = {
      LoggerFactory.getLogger(this.getClass).info(s"scanning .. $toV")
      py.move(toV)
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
