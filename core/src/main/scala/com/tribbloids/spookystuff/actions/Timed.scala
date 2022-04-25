package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.Const
import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.utils.TimeoutConf

import scala.concurrent.duration.Duration

trait Timed extends Action {

  var _timeout: TimeoutConf = _

  def in(timeout: TimeoutConf): this.type = {
    this._timeout = timeout
    this
  }

  def timeout(session: Session): TimeoutConf = {
    val base =
      if (this._timeout == null) session.spooky.spookyConf.remoteResourceTimeout
      else this._timeout

    base
  }

  def hardTerminateTimeout(session: Session): TimeoutConf = {
    val original = timeout(session)
    original.copy(max = original.max + Const.hardTerminateOverhead)
  }

  override def injectFrom(same: ActionLike): Unit = {
    super.injectFrom(same)
    this._timeout = same.asInstanceOf[Timed]._timeout
  }
}
