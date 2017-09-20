package com.tribbloids.spookystuff.uav.actions.mixin

import com.tribbloids.spookystuff.actions.Action

/**
  * unless mixin, assume cost is 0
  */
trait HasCost {
  self: Action =>
}
