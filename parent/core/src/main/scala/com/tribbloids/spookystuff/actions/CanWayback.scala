package com.tribbloids.spookystuff.actions

import java.util.Date

object CanWayback {}

trait CanWayback {

  // TODO change to better time repr
  private var _waybackOvrd: Option[Long] = None

  protected def originalWayback: Option[Long] = None

  def wayback: Option[Long] = _waybackOvrd.orElse(originalWayback)

  final def waybackTo(date: Date): this.type = {
    waybackToTimeMillis(date.getTime)
  }

  final def waybackToTimeMillis(time: Long): this.type = {
    this._waybackOvrd = Some(time)
    this
  }
}
