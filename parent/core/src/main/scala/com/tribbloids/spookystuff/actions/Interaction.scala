package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.doc.Doc
import com.tribbloids.spookystuff.agent.Agent

import scala.concurrent.duration.Duration

@SerialVersionUID(-98257039403274083L)
abstract class Interaction extends Action {

  def cooldown: Duration

  override def doExe(agent: Agent): Seq[Doc] = {

    exeNoOutput(agent: Agent)

    if (cooldown != null && cooldown.toMillis > 0) {
      Thread.sleep(cooldown.toMillis)
    }

    Nil
  }

  def exeNoOutput(agent: Agent): Unit
}
