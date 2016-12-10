package com.tribbloids.spookystuff.mav.actions

import com.tribbloids.spookystuff.session.python.CaseInstanceRef

@SerialVersionUID(-928750192836509428L)
trait WayPoint

@SerialVersionUID(56746829410409L)
case class Global(
                   lat: Double,
                   lon: Double,
                   alt: Double
                 ) extends WayPoint with CaseInstanceRef


@SerialVersionUID(-5039218743229730432L)
case class GlobalRelative(
                           lat: Double,
                           lon: Double,
                           alt: Double
                         ) extends WayPoint with CaseInstanceRef

@SerialVersionUID(4604257236921846832L)
case class Local(
                  north: Double,
                  east: Double,
                  down: Double
                ) extends WayPoint with CaseInstanceRef
