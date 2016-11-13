package com.tribbloids.spookystuff.mav.actions

import com.tribbloids.spookystuff.actions.Interaction
import com.tribbloids.spookystuff.session.Session

/**
  * inbound -> engage -> outbound
  *
  */
trait DroneInteraction extends Interaction {

  override def exeNoOutput(session: Session): Unit = {
    inbound(session)
    engage(session)
    outbound(session)
  }

  // when enclosed in an export, may behave differently.
  def inbound(session: Session): Unit = {}

  def engage(session: Session): Unit = {}

  def outbound(session: Session): Unit = {}
}
