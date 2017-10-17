package com.tribbloids.spookystuff.uav.spatial.util

import com.tribbloids.spookystuff.uav.spatial.{Anchor, SpatialSystem}


case class SearchAttempt[+T <: SpatialSystem](
                                                  from: Anchor,
                                                  system: T,
                                                  to: Anchor
                                                ) {
}