package com.tribbloids.spookystuff.mav

import com.tribbloids.spookystuff.mav.telemetry.{Drone, Link, LinkWithContext}
import org.slf4j.LoggerFactory

/**
  * Created by peng on 12/11/16.
  */
package object dsl {

  type LinkFactory = (Drone => Link)
}
