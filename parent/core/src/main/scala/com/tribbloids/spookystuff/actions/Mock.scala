package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.agent.Agent
import com.tribbloids.spookystuff.doc.Observation

case class Mock(
    observations: Seq[Observation]
) extends Action {

  override protected def doExe(agent: Agent): Seq[Observation] = {

    observations
  }
}
