package com.tribbloids.spookystuff.uav

import com.tribbloids.spookystuff.SpookyException

/**
  * Created by peng on 13/12/16.
  */
class UAVException(
                    override val message: String = "",
                    override val cause: Throwable = null
                  ) extends SpookyException(message, cause) {

}

class ReinforcementDepletedException(
                                      override val message: String = "",
                                      override val cause: Throwable = null
                                    ) extends UAVException(message, cause)