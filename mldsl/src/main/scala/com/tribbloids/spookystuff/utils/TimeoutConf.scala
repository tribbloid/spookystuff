package com.tribbloids.spookystuff.utils

import scala.concurrent.duration._
import scala.language.implicitConversions

case class TimeoutConf(
    max: Duration,
    noProgress: Duration = 30.seconds
) {}

object TimeoutConf {

  implicit def fromDuration(v: Duration): TimeoutConf = TimeoutConf(v)
}
