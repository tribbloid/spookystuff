package com.tribbloids.spookystuff.agent

/**
  * Created by peng on 11/11/16.
  */
class DriverStatus[T](
    val self: T,
    @volatile var isBusy: Boolean = true,
    @volatile var isBroken: Boolean = false
)
