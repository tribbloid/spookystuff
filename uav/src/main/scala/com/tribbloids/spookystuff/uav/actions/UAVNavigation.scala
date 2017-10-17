package com.tribbloids.spookystuff.uav.actions

import com.tribbloids.spookystuff.actions.Interaction
import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.uav.actions.mixin.HasLocation
import com.tribbloids.spookystuff.uav.utils.UAVViews

import scala.language.implicitConversions

/**
  * inbound -> engage -> outbound
  */
trait UAVNavigation extends Interaction
  with UAVAction
  with HasLocation {

  def speedOpt: Option[Double] = None

  override def exeNoOutput(session: Session): Unit = {

    val sv = this.getSessionView(session)
    sv.inbound()
    sv.engage()
    sv.outbound()
  }

  implicit def getSessionView(session: Session) = new NavSessionView(session)
}

class NavSessionView(session: Session) extends UAVViews.SessionView(session) {

  def inbound(): Unit = {}

  def engage(): Unit = {}

  def outbound(): Unit = {}
}