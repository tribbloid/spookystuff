package com.tribbloids.spookystuff.uav.actions.mixin

import com.tribbloids.spookystuff.SpookyContext
import com.tribbloids.spookystuff.actions.{Action, Trace}
import com.tribbloids.spookystuff.uav.spatial.Location

trait HasExactStartEndLocation extends HasStartEndLocations {
  self: Action =>

  override def getStart(trace: Trace, spooky: SpookyContext): Location = _start
  override def getEnd(trace: Trace, spooky: SpookyContext): Location = _end

  def _start: Location
  def _end: Location = _start
}
