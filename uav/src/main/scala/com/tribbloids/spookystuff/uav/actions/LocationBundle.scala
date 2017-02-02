package com.tribbloids.spookystuff.uav.actions

/**
  * Usually get by vehicle.location.***_frame
  */
case class LocationBundle(
                           global: LocationGlobal,
                           globalRelative: LocationGlobalRelative,
                           local: LocationLocal
                         )
