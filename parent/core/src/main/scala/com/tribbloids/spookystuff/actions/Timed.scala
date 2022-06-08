package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.Const
import com.tribbloids.spookystuff.session.Session
import com.tribbloids.spookystuff.utils.Timeout

trait Timed extends Action {

  var _timeout: Timeout = _

  def in(timeout: Timeout): this.type = {
    this._timeout = timeout
    this
  }

  def timeout(session: Session): Timeout = {
    val base =
      if (this._timeout == null) session.spooky.spookyConf.remoteResourceTimeout
      else this._timeout

    base
  }

  def hardTerminateTimeout(session: Session): Timeout = {
    val original = timeout(session)
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
