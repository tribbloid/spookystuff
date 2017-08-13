package com.tribbloids.spookystuff.uav.actions

import com.tribbloids.spookystuff.actions.{Action, Interaction}
import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.uav.{UAVConf, UAVMetrics}
import com.tribbloids.spookystuff.uav.spatial.Location
import com.tribbloids.spookystuff.uav.utils.UAVViews

/**
  * unless mixin, assume cost is 0
  */
trait HasCost {
  self: Action =>
}

/**
  * used by TrafficControl to generate new actions that minimize risk.
  */
trait HasRisk {
  self: Action =>

//  def
}

trait UAVAction extends Action {
  {
    UAVConf
    UAVMetrics
  }
}

/**
  * inbound -> engage -> outbound
  */
trait UAVNavigation extends Interaction with UAVAction with HasCost with HasRisk {

  def _to: Location
  def _from: Location = _to

  def speedOpt: Option[Double] = None

  override def exeNoOutput(session: Session): Unit = {

    val sv = this.getSessionView(session)
    sv.inbound()
    sv.engage()
    sv.outbound()
  }

  def getSessionView(session: Session) = new this.SessionView(session)

  implicit class SessionView(session: Session) extends UAVViews.SessionView(session) {

    /**
      * when enclosed in an export, may behave differently.
      */
    def inbound(): Unit = {}

    def engage(): Unit = {}

    def outbound(): Unit = {}
  }
}