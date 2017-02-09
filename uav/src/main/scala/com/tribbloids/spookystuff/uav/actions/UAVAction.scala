package com.tribbloids.spookystuff.uav.actions

import com.tribbloids.spookystuff.actions.{Action, Interaction}
import com.tribbloids.spookystuff.uav.UAVConf
import com.tribbloids.spookystuff.uav.system.Drone
import com.tribbloids.spookystuff.uav.telemetry.Link
import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.uav.spatial.LocationGlobal

trait UAVAction extends Action {

  // override this to enforce selection over drones being deployed.
  // drone that yield higher preference will be used if available, regardless of whether its in the air or not.
  // drone that yield None will never be used. TODO enable later
  //  def preference: DronePreference = {_ => Some(1)}

  class SessionView(session: Session) {

    val mavConf: UAVConf = {
      session.spooky.conf.submodule[UAVConf]
    }

    def effectiveDrones: Seq[Drone] = mavConf.drones

    val link: Link = {
      Link.trySelect(
        effectiveDrones,
        session
      )
        .get
    }
  }
}

/**
  * inbound -> engage -> outbound
  */
trait UAVPositioning extends Interaction with UAVAction {

  def start_end: Seq[(LocationGlobal, LocationGlobal)]
  def speed: Double = 5.0

  override def exeNoOutput(session: Session): Unit = {

    val sv = this.getSessionView(session)
    sv.inbound()
    sv.engage()
    sv.outbound()
  }

  def getSessionView(session: Session) = new this.SessionView(session)

  class SessionView(session: Session) extends super.SessionView(session) {
    /**
      * when enclosed in an export, may behave differently.
      */
    def inbound(): Unit = {}

    def engage(): Unit = {}

    def outbound(): Unit = {}
  }
}