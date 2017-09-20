package com.tribbloids.spookystuff.uav.actions

import com.tribbloids.spookystuff.actions.Interaction
import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.uav.actions.mixin.HasStartEndLocations
import com.tribbloids.spookystuff.uav.utils.UAVViews

/**
  * inbound -> engage -> outbound
  */
trait UAVNavigation extends Interaction
  with UAVAction
  with HasStartEndLocations {

  def speedOpt: Option[Double] = None

  override def exeNoOutput(session: Session): Unit = {

    val sv = this.getSessionView(session)
    sv.inbound()
    sv.engage()
    sv.outbound()
  }

  def getSessionView(session: Session) = new this.SessionView(session)

  implicit class SessionView(session: Session) extends UAVViews.SessionView(session) {

    def inbound(): Unit = {}

    def engage(): Unit = {}

    def outbound(): Unit = {}
  }
}
