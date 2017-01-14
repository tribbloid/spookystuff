package com.tribbloids.spookystuff.mav

import com.tribbloids.spookystuff.SpookyConf
import com.tribbloids.spookystuff.mav.telemetry.{Drone, Link}

/**
  * Created by peng on 12/11/16.
  */
package object dsl {

//  type Fleet = () => Seq[Drone] //=> Seq[Drone, hostname={ip:port}]

  type LinkFactory = (Drone => Link)
}
