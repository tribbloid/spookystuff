package com.tribbloids.spookystuff.uav

import com.tribbloids.spookystuff.SpookyException

/**
  * Created by peng on 13/12/16.
  */
class UAVException(
    val simpleMsg: String = "",
    override val cause: Throwable = null
) extends SpookyException {}

class LinkDepletedException(
    override val simpleMsg: String = "",
    override val cause: Throwable = null
) extends UAVException(simpleMsg, cause)
