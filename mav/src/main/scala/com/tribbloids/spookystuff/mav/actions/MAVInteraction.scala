package com.tribbloids.spookystuff.mav.actions

import com.tribbloids.spookystuff.actions.{Action, Interaction}
import com.tribbloids.spookystuff.mav.MAVConf
import com.tribbloids.spookystuff.mav.telemetry.Link
import com.tribbloids.spookystuff.session.Session

trait MAVAction extends Action {

  class SessionView(session: Session) {

    val mavConf: MAVConf = {
      session.spooky.conf.submodule[MAVConf]
    }

    val link: Link = {
      Link.getOrCreate(
        mavConf.drones,
        mavConf.linkFactory,
        session
      )
    }

    def endpoint = link.Endpoints.primary
    def py = endpoint.Py(session)
  }
}

/**
  * inbound -> engage -> outbound
  *
  */
trait MAVInteraction extends Interaction with MAVAction {

  override def exeNoOutput(session: Session): Unit = {

    val sv = this.getSessionView(session)
    sv.link.Py(session).$Helpers.withDaemonsUp {
      sv.inbound()
      sv.conduct()
      sv.outbound()
    }
  }

  def getSessionView(session: Session) = new this.SessionView(session)

  class SessionView(session: Session) extends super.SessionView(session) {
    /**
      * when enclosed in an export, may behave differently.
      */
    def inbound(): Unit = {}

    def conduct(): Unit = {}

    def outbound(): Unit = {}
  }
}