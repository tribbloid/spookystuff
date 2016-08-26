package com.tribbloids.spookystuff.mav

import com.tribbloids.spookystuff.actions.{Action, Interaction}
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


case class MovePath(
                     from: WayPoint,
                     to: WayPoint,
                     override val delay: Duration
                   ) extends Interaction(delay, true) {

  override def exeWithoutPage(session: Session): Unit = ???
}