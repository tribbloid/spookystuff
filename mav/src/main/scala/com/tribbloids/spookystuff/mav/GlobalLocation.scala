package com.tribbloids.spookystuff.mav

import com.tribbloids.spookystuff.actions.Interaction
import com.tribbloids.spookystuff.session.Session

import scala.concurrent.duration.Duration

/**
  * Created by peng on 14/08/16.
  */
trait WayPoint

case class GlobalLocation(
                           lat: Double,
                           lon: Double,
                           alt: Double,
                           relative: Boolean = false //set to 'true' to make altitude relative to home
                         ) extends WayPoint {

}

abstract class DroneInteraction extends Interaction

case class Move(
                 from: WayPoint,
                 to: WayPoint,
                 override val delay: Duration
               ) extends DroneInteraction {

  override def exeNoOutput(session: Session): Unit = {

    val python = session.pythonDriver
    val result = python.interpret("spookystuff.mav")
  }
}