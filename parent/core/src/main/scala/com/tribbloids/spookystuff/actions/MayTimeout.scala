package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.agent.Agent
import com.tribbloids.spookystuff.commons.Timeout

trait MayTimeout {
  self: Action =>

  var _timeout: Timeout = _ // TODO: how to make it immutable?

  def in(timeout: Timeout): this.type = {
    this._timeout = timeout
    this
  }

  def getTimeout(agent: Agent): Timeout =
    Option(MayTimeout.this._timeout).getOrElse(agent.spooky.conf.remoteResourceTimeout)

//  override def injectFrom(same: ActionLike): Unit = {
//    super.injectFrom(same)
//    this._timeout = same.asInstanceOf[Timed]._timeout
//  }
}

object MayTimeout {

  // TODO: remove, irrelevant, action will only be accessed by 1 thread
//  trait ThreadSafe extends Timed
//  trait ThreadUnsafe extends Timed
}
