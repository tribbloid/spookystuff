package com.tribbloids.spookystuff.uav.actions.mixin

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions.{Action, Trace}
import com.tribbloids.spookystuff.uav.spatial.Location

/**
  * unless mixin, assume cost is 0
  */
trait HasStartEndLocations {
  self: Action =>

  def getStart(trace: Trace, spooky: SpookyContext): Location
  def getEnd(trace: Trace, spooky: SpookyContext): Location = getStart(trace, spooky)
}
