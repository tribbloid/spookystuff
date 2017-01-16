package com.tribbloids.spookystuff.mav.actions

import com.tribbloids.spookystuff.actions.{Action, Interaction}
import com.tribbloids.spookystuff.mav.MAVConf
import com.tribbloids.spookystuff.mav.hardware.Drone
import com.tribbloids.spookystuff.mav.telemetry.Link
import com.tribbloids.spookystuff.session.Session

trait MAVAction extends Action {

  // override this to enforce selection over drones being deployed.
  // drone that yield higher preference will be used if available, regardless of whether its in the air or not.
  // drone that yield None will never be used. TODO enable later
//  def preference: DronePreference = {_ => Some(1)}
  def preference: Seq[Drone] = Nil

  class SessionView(session: Session) {

    val mavConf: MAVConf = {
      session.spooky.conf.submodule[MAVConf]
    }

    val drones = if (preference.isEmpty) mavConf.drones
    else preference

    val link: Link = {
      Link.getOrCreate(
        drones,
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