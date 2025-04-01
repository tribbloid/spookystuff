package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.actions.HasTrace.StateChangeTag

import java.util.Date

object Wayback {

  implicit class _Ops[T <: Wayback](self: T) {

    final def waybackTo(date: Date): T = {
      waybackToTimeMillis(date.getTime)
    }

    final def waybackToTimeMillis(time: Long): T = {

      val copied = self.deepCopy()

      self._waybackOvrd = Some(time)
      self
    }
  }
}

trait Wayback extends Action {
  self: StateChangeTag =>

  // TODO change to better time repr
  private var _waybackOvrd: Option[Long] = None

  protected def originalWayback: Option[Long] = None

  def wayback: Option[Long] = _waybackOvrd.orElse(originalWayback)

}
