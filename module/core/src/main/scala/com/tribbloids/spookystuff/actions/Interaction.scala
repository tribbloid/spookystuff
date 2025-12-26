package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.doc.Doc
import com.tribbloids.spookystuff.agent.Harness
import com.tribbloids.spookystuff.tool.HasTrace

import scala.concurrent.duration.Duration

@SerialVersionUID(-98257039403274083L) // TODO: should be "DriverInteraction"
abstract class Interaction extends Action {

  def cooldown: Duration

  override def doExe(agent: Harness): Seq[Doc] = {

    exeNoOutput(agent: Harness)

    if (cooldown != null && cooldown.toMillis > 0) {
      Thread.sleep(cooldown.toMillis)
    }

    Nil
  }

  def exeNoOutput(agent: Harness): Unit

  override def stateChangeOnly: HasTrace = this
}
