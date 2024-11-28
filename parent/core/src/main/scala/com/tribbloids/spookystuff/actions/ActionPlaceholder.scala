package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.agent.Agent
import com.tribbloids.spookystuff.doc.Observation

trait ActionPlaceholder extends Action {

  override protected[actions] def doExe(agent: Agent): Seq[Observation] = {
    throw new UnsupportedOperationException(s"${this.getClass.getSimpleName} is a placeholder")
  }
}
