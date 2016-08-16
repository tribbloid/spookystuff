package com.tribbloids.spookystuff.mav

/**
  * Created by peng on 14/08/16.
  */
trait WayPoint


case class WayPath(
                    from: WayPoint,
                    to: WayPoint
                  )

case class GlobalLocation(
                           lat: Double,
                           lon: Double,
                           alt: Double,
                           relative: Boolean = false //set to 'true' to make altitude relative to home
                         ) extends WayPoint {

}