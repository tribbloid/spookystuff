package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.actions.Action.Driverless
import com.tribbloids.spookystuff.agent.Agent
import com.tribbloids.spookystuff.doc.Observation

case class Mock( // TODO: should be export
    observations: Seq[Observation]
) extends Export
    with Driverless {

  override def doExe(agent: Agent): Seq[Observation] = {

    observations
  }
}
