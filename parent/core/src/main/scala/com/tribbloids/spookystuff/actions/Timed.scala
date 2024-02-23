package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.Const
import com.tribbloids.spookystuff.agent.Agent
import com.tribbloids.spookystuff.commons.Timeout

trait Timed extends Action {

  var _timeout: Timeout = _

  def in(timeout: Timeout): this.type = {
    this._timeout = timeout
    this
  }

  def timeout(agent: Agent): Timeout = {
    val base =
      if (this._timeout == null) agent.spooky.conf.remoteResourceTimeout
      else this._timeout

    base
  }

  def hardTerminateTimeout(agent: Agent): Timeout = {
    val original = timeout(agent)
    original.copy(max = original.max + Const.hardTerminateOverhead)
  }

  override def injectFrom(same: ActionLike): Unit = {
    super.injectFrom(same)
    this._timeout = same.asInstanceOf[Timed]._timeout
  }
}

object Timed {

  trait ThreadSafe extends Timed

  trait ThreadUnsafe extends Timed
}
