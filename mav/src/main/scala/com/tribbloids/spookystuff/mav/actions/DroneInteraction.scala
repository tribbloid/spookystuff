package com.tribbloids.spookystuff.mav.actions

import com.tribbloids.spookystuff.actions.{Action, Interaction}
import com.tribbloids.spookystuff.session.{AbstractSession, Session}

trait DroneAction extends Action

/**
  * inbound -> engage -> outbound
  *
  */
trait DroneInteraction extends Interaction with DroneAction {

  override def exeNoOutput(session: AbstractSession): Unit = {
    val s = session.asInstanceOf[Session]
    inbound(s)
    conduct(s)
    outbound(s)
  }

  /**
    * when enclosed in an export, may behave differently.
    */
  def inbound(session: Session): Unit = {}

  def conduct(session: Session): Unit = {}

  def outbound(session: Session): Unit = {}
}
