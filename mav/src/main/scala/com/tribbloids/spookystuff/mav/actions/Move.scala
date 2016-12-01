package com.tribbloids.spookystuff.mav.actions

import scala.concurrent.duration.Duration

/**
  * Created by peng on 26/08/16.
  * Go to point1 then go to point2, end of.
  */
// How to accommodate camera & gimbal control? Right now do not refactor! Simplicity first.
case class Move(
                 from: WayPoint,
                 to: WayPoint,
                 override val delay: Duration = null
               ) extends DroneInteraction {

  def inbound(): Unit = {

  }
}