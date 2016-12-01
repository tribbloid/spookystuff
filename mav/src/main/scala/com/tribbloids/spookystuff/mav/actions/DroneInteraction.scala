package com.tribbloids.spookystuff.mav.actions

import com.tribbloids.spookystuff.actions.{Action, Interaction}
import com.tribbloids.spookystuff.session.AbstractSession

trait DroneAction extends Action

/**
  * inbound -> engage -> outbound
  *
  */
trait DroneInteraction extends Interaction with DroneAction {

  override def exeNoOutput(session: AbstractSession): Unit = {
    inbound(session)
    engage(session)
    outbound(session)
  }

  // when enclosed in an export, may behave differently.
  def inbound(session: AbstractSession): Unit = {}

  def engage(session: AbstractSession): Unit = {}

  def outbound(session: AbstractSession): Unit = {}
}
