package com.tribbloids.spookystuff.utils

import scala.concurrent.duration._
import scala.language.implicitConversions

case class Timeout(
    max: Duration,
    noProgress: Duration = 30.seconds
) {

  override lazy val toString: String =
    s"[$max / ${noProgress} if no progress]"
}

object Timeout {

  implicit def fromDuration(v: Duration): Timeout = Timeout(v)
}
