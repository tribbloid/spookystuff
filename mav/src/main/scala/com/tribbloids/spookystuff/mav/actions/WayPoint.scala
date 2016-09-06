package com.tribbloids.spookystuff.mav.actions

@SerialVersionUID(-928750192836509428L)
case class WayPoint(
                     globalLocation: Option[GlobalLocation]
                   )

@SerialVersionUID(56746829410409L)
case class GlobalLocation(
                           lat: Double,
                           lon: Double,
                           alt: Double,
                           relative: Boolean = false //set to 'true' to make altitude relative to home
                         )