package com.tribbloids.spookystuff.actions

import com.tribbloids.spookystuff.actions.Wayback.WaybackLike

import java.util.Date

object Wayback {

  trait WaybackLike {

    def wayback: Option[Long]
  }
}

trait Wayback extends WaybackLike {

  // TODO change to immutable
  // TODO change to better time repr
  var wayback: Option[Long] = _

  def waybackTo(date: Date): this.type = {
    waybackToTimeMillis(date.getTime)
  }

  def waybackToTimeMillis(time: Long): this.type = {
    this.wayback = Some(time)
    this
  }
}
