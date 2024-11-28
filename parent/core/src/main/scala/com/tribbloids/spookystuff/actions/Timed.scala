package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.Const
import com.tribbloids.spookystuff.agent.Agent
import com.tribbloids.spookystuff.commons.Timeout
import com.tribbloids.spookystuff.doc.Observation

trait Timed extends Action {

  def in(timeout: Timeout): Timed.Explicitly[this.type] = {
    Timed.Explicitly(this, timeout)
  }

  def getTimeout(agent: Agent): Timeout = {
    agent.spooky.conf.remoteResourceTimeout
  }

  def getTimeout_hardTerminate(agent: Agent): Timeout = {
    val original = getTimeout(agent)
    original.copy(max = original.max + Const.hardTerminateOverhead)
  }

}

object Timed {

  case class Explicitly[T <: Timed](
      delegate: T,
      timeout: Timeout
  ) extends Timed {

    override def getTimeout(agent: Agent): Timeout = {
      timeout
    }

    override protected[actions] def doExe(agent: Agent): Seq[Observation] = {
      delegate.doExe(agent)
    }
  }

  trait ThreadSafe extends Timed
}
