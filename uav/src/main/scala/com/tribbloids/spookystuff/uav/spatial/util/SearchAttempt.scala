package com.tribbloids.spookystuff.uav.spatial.util

import com.tribbloids.spookystuff.uav.spatial.Anchor
import com.tribbloids.spookystuff.uav.spatial.point.CoordinateSystem

case class SearchAttempt[+T <: CoordinateSystem](
    from: Anchor,
    system: T,
    to: Anchor
) {}
